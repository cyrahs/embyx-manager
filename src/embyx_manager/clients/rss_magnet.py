"""Extract magnet links embedded in feed item bodies."""

import logging
from urllib.parse import unquote

import humanfriendly
from pyquery import PyQuery

log = logging.getLogger('embyx-manager.rss-magnet')

_NOTICE_LEVEL = 25


def get_magnet_from_html(content: str, avid: str) -> str | None:
    """Pick the largest magnet from the HTML table inside one feed item's body.

    JavBus-style feeds (RSSHub's javbus route) render the magnet listing as a
    table of link, size and date; feeds without one simply yield nothing.
    """
    if not content:
        return None
    rows = PyQuery(content)('table tbody tr').filter(lambda _, el: PyQuery(el)("a[href^='magnet:']").length)

    results = []
    for row in rows.items():
        anchor = row("td:nth-child(1) a[href^='magnet:']")
        magnet = anchor.attr('href')
        size = row('td:nth-child(2)').text().strip()
        try:
            size_int = humanfriendly.parse_size(size)
        except humanfriendly.InvalidSize:
            log.warning('Skipping feed magnet with invalid size %s for %s', size, avid)
            continue
        try:
            magnet, name = magnet.split('&dn=')
            name = unquote(name)
        except ValueError:
            magnet = magnet.split('&')[0]
            name = 'Unknown'
        results.append({'magnet': f'{magnet}&dn={avid}', 'size': size, 'name': name, 'size_int': size_int})
    if not results:
        return None
    results.sort(key=lambda x: x['size_int'], reverse=True)
    log.log(_NOTICE_LEVEL, 'Found %d results for %s from the feed item:', len(results), avid)
    log_lines: list[str] = ['Display top 5 largest:']
    for index, result in enumerate(results[:5]):
        mark = '✅' if index == 0 else '--'
        log_lines.append(f'{mark}--{result["size"]} {result["name"]}')
        log_lines.append(result['magnet'])
    log.info('\n'.join(log_lines))
    return results[0]['magnet']
