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

ADDRESS_LINE_RE = re.compile(
    r"""
    ^\s*
    \d{1,8}
    [^,\n]{2,160}
    ,\s*
    [A-Za-z0-9 .'\-]{2,80}
    ,\s*
    CA\s*
    \d{5}(?:-\d{4})?
    \s*$
    """,
    re.VERBOSE,
)
CA_ZIP_RE = re.compile(r"\bCA\s+\d{5}(?:-\d{4})?\b")


def generate_ca_zip_seeds(step: int = 25, jitter: bool = True) -> List[str]:
    start = CA_ZIP_MIN + (step // 2) if jitter else CA_ZIP_MIN
    return [f"{z:05d}" for z in range(start, CA_ZIP_MAX + 1, step)]


def extract_ca_addresses(sidebar_text: str) -> List[str]:
    lines = [ln.strip() for ln in sidebar_text.splitlines() if ln.strip()]
    out: List[str] = []

    for ln in lines:
        if ADDRESS_LINE_RE.match(ln):
            out.append(ln)

    block: List[str] = []
    for ln in lines:
        block.append(ln)
        if CA_ZIP_RE.search(ln):
            for k in (1, 2, 3, 4):
                if len(block) >= k:
                    cand = " ".join(block[-k:])
                    cand = re.sub(r"\s+", " ", cand).strip()
                    if ADDRESS_LINE_RE.match(cand):
                        out.append(cand)
                        break
            block = []
        if len(block) > 8:
            block = block[-4:]

    seen_local = set()
    normed = []
    for a in out:
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2 not in seen_local:
            seen_local.add(a2)
            normed.append(a2)
    return normed


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


async def wait_sidebar_change(frame: Frame, before: str, timeout_ms: int = 15000) -> str:
    deadline = frame.page.context._loop.time() + timeout_ms / 1000.0
    while frame.page.context._loop.time() < deadline:
        now = await sidebar_snapshot(frame)
        if now and now != (before or ""):
            return now
        await frame.wait_for_timeout(200)
    return await sidebar_snapshot(frame)


async def trigger_search(frame: Frame) -> None:
    ok = False
    try:
        ok = await frame.evaluate("() => (typeof searchLocations === 'function')")
    except Exception:
        ok = False

    if ok:
        await frame.evaluate("() => { searchLocations(); }")
    else:
        await frame.wait_for_selector(BUTTON_SELECTOR, timeout=15000)
        await frame.click(BUTTON_SELECTOR)


async def set_query_and_search(frame: Frame, query: str) -> str:
    """
    Deterministic submit:
      - ensure radius=50
      - type query
      - wait until input.value matches query (using arg=)
      - trigger_search
      - wait for sidebar change
    Returns: 'ok' or 'failed'
    """
    await ensure_radius_50(frame)

    before = await sidebar_snapshot(frame)

    await frame.fill(INPUT_SELECTOR, "")
    await frame.type(INPUT_SELECTOR, query, delay=20)

    await frame.wait_for_function(
        """(v) => {
            const el = document.querySelector('#addressInput');
            return el && el.value && el.value.trim() === v;
        }""",
        arg=query,
        timeout=5000,
    )

    await trigger_search(frame)
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
        print(f"CA ZIP seeds: {len(zips)} (range {CA_ZIP_MIN}-{CA_ZIP_MAX}, step={zip_step})")
        print(f"Existing CA addresses loaded: {len(seen)}")

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

            mode1 = await set_query_and_search(frame, z)
            if mode1 == "failed":
                mode2 = await set_query_and_search(frame, f"{z}, CA")
                mode = "zip_ca" if mode2 == "ok" else "failed"
            else:
                mode = "zip"

            sidebar_text = await sidebar_snapshot(frame)
            addrs = extract_ca_addresses(sidebar_text) if sidebar_text else []
            new_addrs = [a for a in addrs if a not in seen]

            if new_addrs:
                with out_path.open("a", encoding="utf-8") as f:
                    for a in new_addrs:
                        a_clean = a.replace("\n", " ").replace("\r", " ").strip()
                        f.write(a_clean + "\n")
                        seen.add(a_clean)
                        print(f"NEW: {a_clean}")

            print(f"[{i+1}] zip={z} mode={mode} ca_results={len(addrs)} unique={len(seen)} +{len(new_addrs)}")
            await page.wait_for_timeout(delay_ms)

        await browser.close()


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="ca_unique_addresses.csv")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--delay_ms", type=int, default=450)
    ap.add_argument("--max_queries", type=int, default=0, help="0 means no limit.")
    ap.add_argument("--zip_step", type=int, default=25)
    ap.add_argument("--reload_every", type=int, default=20)
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
