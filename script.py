import asyncio
import re
from pathlib import Path
from typing import List, Optional, Set

from playwright.async_api import async_playwright, Page, Frame

URL = "https://eziz.org/vfc/provider-locations/"
IFRAME_SELECTOR = 'iframe[src*="vfc-provider-locations.html"]'
INPUT_SELECTOR = "#addressInput"
RADIUS_SELECTOR = "#radiusSelect"
BUTTON_SELECTOR = 'input[type="button"][value="Find Providers"]'
SIDEBAR_SELECTOR = "#sidebar"

CA_ZIP_MIN = 90000
CA_ZIP_MAX = 96199

ZIP_ANCHOR = re.compile(r"\bCA\s+9\d{4}(?:-\d{4})?\b", re.IGNORECASE)
MILES_ANYWHERE = re.compile(r"\(\s*\d+(\.\d+)?\s*\)\s*miles?\.?", re.IGNORECASE)
PHONE_ANYWHERE = re.compile(r"Phone:\s*\(\d{3}\)\s*\d{3}-\d{4}", re.IGNORECASE)
GET_DIRECTIONS_ANYWHERE = re.compile(r"Get\s+Directions\s*", re.IGNORECASE)

ADDRESS_EXTRACT = re.compile(
    r"(\d{1,8}\s+.+?,\s*[^,]+?,\s*CA\s+9\d{4}(?:-\d{4})?)",
    re.IGNORECASE,
)


def generate_ca_zip_seeds(step: int, jitter: bool) -> List[str]:
    start = CA_ZIP_MIN + (step // 2) if jitter else CA_ZIP_MIN
    return [f"{z:05d}" for z in range(start, CA_ZIP_MAX + 1, step)]


def norm(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+,", ",", s)
    s = re.sub(r",\s+", ", ", s)
    return s


def extract_address_only(line_or_block: str) -> Optional[str]:
    s = norm(line_or_block)
    s = GET_DIRECTIONS_ANYWHERE.sub("", s)
    s = MILES_ANYWHERE.sub("", s)
    s = PHONE_ANYWHERE.sub("", s)
    s = norm(s)

    m = ADDRESS_EXTRACT.search(s)
    if m:
        return norm(m.group(1))

    if ZIP_ANCHOR.search(s) and re.search(r"\d", s):
        idx = s.lower().rfind(", ca ")
        if idx != -1:
            return norm(s[:].strip())

    return None


def extract_unique_addresses(sidebar_text: str) -> List[str]:
    raw_lines = [ln.strip() for ln in sidebar_text.splitlines() if ln.strip()]
    cleaned_lines = [norm(ln) for ln in raw_lines]

    out: List[str] = []

    for ln in cleaned_lines:
        if ZIP_ANCHOR.search(ln):
            a = extract_address_only(ln)
            if a:
                out.append(a)

    block: List[str] = []
    for ln in cleaned_lines:
        block.append(ln)
        if len(block) > 5:
            block = block[-5:]
        if ZIP_ANCHOR.search(ln):
            for k in (2, 3, 4, 5):
                if len(block) >= k:
                    cand = " ".join(block[-k:])
                    a = extract_address_only(cand)
                    if a:
                        out.append(a)
                        break

    seen = set()
    uniq = []
    for a in out:
        a2 = norm(a)
        if a2 and a2 not in seen:
            seen.add(a2)
            uniq.append(a2)
    return uniq


async def accept_common_banners(page: Page) -> None:
    for name in ("Accept", "I Agree", "Agree", "OK", "Got it"):
        try:
            await page.get_by_role("button", name=name).click(timeout=1200)
            return
        except Exception:
            pass


async def get_frame(page: Page) -> Frame:
    await page.wait_for_selector(IFRAME_SELECTOR, timeout=45000)
    iframe_el = await page.query_selector(IFRAME_SELECTOR)
    if iframe_el is None:
        raise RuntimeError("Iframe not found.")
    frame = await iframe_el.content_frame()
    if frame is None:
        raise RuntimeError("Iframe content_frame() is None.")
    return frame


async def ensure_radius_50(frame: Frame) -> None:
    await frame.wait_for_selector(RADIUS_SELECTOR, timeout=20000)
    await frame.select_option(RADIUS_SELECTOR, value="50")


async def sidebar_snapshot(frame: Frame) -> str:
    try:
        await frame.wait_for_selector(SIDEBAR_SELECTOR, timeout=15000)
        return (await frame.locator(SIDEBAR_SELECTOR).inner_text()).strip()
    except Exception:
        return ""


async def wait_sidebar_change(frame: Frame, before: str, timeout_ms: int = 12000) -> str:
    deadline = frame.page.context._loop.time() + timeout_ms / 1000.0
    while frame.page.context._loop.time() < deadline:
        now = await sidebar_snapshot(frame)
        if now and now != (before or ""):
            return now
        await frame.wait_for_timeout(120)
    return await sidebar_snapshot(frame)


async def trigger_search_fast(frame: Frame) -> None:
    try:
        has_fn = await frame.evaluate("() => (typeof searchLocations === 'function')")
    except Exception:
        has_fn = False

    if has_fn:
        await frame.evaluate("() => { searchLocations(); }")
    else:
        await frame.wait_for_selector(BUTTON_SELECTOR, timeout=15000)
        await frame.evaluate(
            """() => {
                const btn = document.querySelector('input[type="button"][value="Find Providers"]');
                if (btn) btn.click();
            }"""
        )


async def set_query_and_search_fast(frame: Frame, query: str) -> str:
    await ensure_radius_50(frame)
    before = await sidebar_snapshot(frame)

    await frame.evaluate(
        """(q) => {
            const el = document.querySelector('#addressInput');
            if (!el) return;
            el.value = q;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        arg=query,
    )

    await trigger_search_fast(frame)
    after = await wait_sidebar_change(frame, before, timeout_ms=12000)
    return "ok" if after and after != before else "failed"


async def run(
    out_csv: str,
    headless: bool,
    delay_ms: int,
    max_queries: Optional[int],
    zip_step: int,
    reload_every: int,
) -> None:
    out_path = Path(out_csv).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    seen: Set[str] = set()
    if out_path.exists():
        for ln in out_path.read_text(encoding="utf-8").splitlines()[1:]:
            ln = ln.strip()
            if ln:
                seen.add(ln)

    if not out_path.exists():
        out_path.write_text("address\n", encoding="utf-8")

    zips = generate_ca_zip_seeds(step=zip_step, jitter=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(URL, wait_until="domcontentloaded")
        await accept_common_banners(page)

        frame = await get_frame(page)
        await frame.wait_for_selector(INPUT_SELECTOR, timeout=30000)
        await ensure_radius_50(frame)

        print(f"Output CSV: {out_path}")
        print(f"CA ZIP seeds: {len(zips)} (step={zip_step})")
        print(f"Existing addresses loaded: {len(seen)}")

        for i, z in enumerate(zips):
            if max_queries is not None and i >= max_queries:
                break

            if i > 0 and reload_every > 0 and i % reload_every == 0:
                print(f"[reload] after {i} queries")
                await page.reload(wait_until="domcontentloaded")
                await accept_common_banners(page)
                frame = await get_frame(page)
                await frame.wait_for_selector(INPUT_SELECTOR, timeout=30000)
                await ensure_radius_50(frame)

            mode1 = await set_query_and_search_fast(frame, z)
            mode = "zip" if mode1 == "ok" else "failed"

            if mode1 == "failed":
                mode2 = await set_query_and_search_fast(frame, f"{z}, CA")
                mode = "zip_ca" if mode2 == "ok" else "failed"

            sidebar_text = await sidebar_snapshot(frame)
            addrs = extract_unique_addresses(sidebar_text) if sidebar_text else []
            new_addrs = [a for a in addrs if a not in seen]

            if new_addrs:
                with out_path.open("a", encoding="utf-8") as f:
                    for a in new_addrs:
                        a_clean = norm(a)
                        f.write(a_clean + "\n")
                        seen.add(a_clean)
                        print(f"NEW: {a_clean}")

            print(f"[{i+1}] zip={z} mode={mode} found={len(addrs)} unique={len(seen)} +{len(new_addrs)}")

            if delay_ms > 0:
                await page.wait_for_timeout(delay_ms)

        await browser.close()


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="ca_unique_addresses.csv")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--delay_ms", type=int, default=150)
    ap.add_argument("--max_queries", type=int, default=0)
    ap.add_argument("--zip_step", type=int, default=5)
    ap.add_argument("--reload_every", type=int, default=25)
    args = ap.parse_args()

    asyncio.run(
        run(
            out_csv=args.out,
            headless=not args.headful,
            delay_ms=args.delay_ms,
            max_queries=None if args.max_queries <= 0 else args.max_queries,
            zip_step=args.zip_step,
            reload_every=args.reload_every,
        )
    )
