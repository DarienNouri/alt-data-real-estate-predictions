import click

from scraper import run_scraper

ZILLOW_DEFAULT_START_URLS = {
    'NYC': 'https://www.zillow.com/new-york-ny/2_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A2%7D%2C%22usersSearchTerm%22%3A%22New%20York%2C%20NY%22%2C%22mapBounds%22%3A%7B%22west%22%3A-74.70065878320312%2C%22east%22%3A-73.25870321679687%2C%22south%22%3A40.289510081853976%2C%22north%22%3A41.10370531208385%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A6181%2C%22regionType%22%3A6%7D%5D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A300000%7D%2C%22mp%22%3A%7B%22min%22%3A1553%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%7D',
    'CT': 'https://www.zillow.com/ct/2_p/?searchQueryState=%7B%22usersSearchTerm%22%3A%22New%20York%2C%20NY%22%2C%22mapBounds%22%3A%7B%22north%22%3A42.325730832862234%2C%22east%22%3A-71.31555143359375%2C%22south%22%3A40.66988259500998%2C%22west%22%3A-74.19946256640625%7D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A200000%7D%2C%22mp%22%3A%7B%22min%22%3A2048%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A11%2C%22regionType%22%3A2%7D%5D%2C%22mapZoom%22%3A9%2C%22pagination%22%3A%7B%22currentPage%22%3A2%7D%7D',
    'RI': 'https://www.zillow.com/ri/2_p/?searchQueryState=%7B%22usersSearchTerm%22%3A%22New%20York%2C%20NY%22%2C%22mapBounds%22%3A%7B%22north%22%3A42.380880444913345%2C%22east%22%3A-70.05595893359376%2C%22south%22%3A40.726461349988604%2C%22west%22%3A-72.93987006640626%7D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A100000%7D%2C%22mp%22%3A%7B%22min%22%3A513%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A9%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A50%2C%22regionType%22%3A2%7D%5D%2C%22pagination%22%3A%7B%22currentPage%22%3A2%7D%7D',
    'NY': 'https://www.zillow.com/ny/2_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A2%7D%2C%22usersSearchTerm%22%3A%22NY%22%2C%22mapBounds%22%3A%7B%22west%22%3A-79.911886203125%2C%22east%22%3A-71.628194796875%2C%22south%22%3A40.07057084687667%2C%22north%22%3A45.391566871346015%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A43%2C%22regionType%22%3A2%7D%5D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A300000%7D%2C%22mp%22%3A%7B%22min%22%3A1436%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A7%7D',
    'VA': 'https://www.zillow.com/va/2_p/?searchQueryState=%7B%22usersSearchTerm%22%3A%22FL%22%2C%22mapBounds%22%3A%7B%22north%22%3A41.32336725858775%2C%22east%22%3A-73.65310273437503%2C%22south%22%3A34.55651770392451%2C%22west%22%3A-85.18874726562503%7D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A300000%7D%2C%22mp%22%3A%7B%22min%22%3A1549%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A7%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A56%2C%22regionType%22%3A2%7D%5D%2C%22pagination%22%3A%7B%22currentPage%22%3A2%7D%7D',
    'FL': 'https://www.zillow.com/fl/2_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A2%7D%2C%22usersSearchTerm%22%3A%22FL%22%2C%22mapBounds%22%3A%7B%22west%22%3A-87.634896%2C%22east%22%3A-79.974306%2C%22south%22%3A24.396308%2C%22north%22%3A31.000968%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A14%2C%22regionType%22%3A2%7D%5D%2C%22isMapVisible%22%3Afalse%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A300000%7D%2C%22mp%22%3A%7B%22min%22%3A1549%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%7D',
    'LA': 'https://www.zillow.com/los-angeles-county-ca/2_p/?searchQueryState=%7B%22usersSearchTerm%22%3A%22Los%20Angeles%2C%20CA%22%2C%22mapBounds%22%3A%7B%22north%22%3A35.47861182891958%2C%22east%22%3A-115.4147513671875%2C%22south%22%3A32.073314284620196%2C%22west%22%3A-121.1825736328125%7D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22min%22%3A5000000%7D%7D%2C%22isListVisible%22%3Atrue%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A3101%2C%22regionType%22%3A4%7D%5D%2C%22pagination%22%3A%7B%22currentPage%22%3A2%7D%2C%22mapZoom%22%3A8%7D',
    'TX': 'https://www.zillow.com/tx/2_p/?searchQueryState=%7B%22usersSearchTerm%22%3A%22FL%22%2C%22mapBounds%22%3A%7B%22north%22%3A36.500704%2C%22east%22%3A-93.508039%2C%22south%22%3A25.837164%2C%22west%22%3A-106.645646%7D%2C%22isMapVisible%22%3Afalse%2C%22filterState%22%3A%7B%22price%22%3A%7B%22min%22%3A300000%7D%2C%22mp%22%3A%7B%22min%22%3A1549%7D%2C%22sort%22%3A%7B%22value%22%3A%22pricea%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A54%2C%22regionType%22%3A2%7D%5D%2C%22pagination%22%3A%7B%22currentPage%22%3A2%7D%7D',
}


@click.command()
@click.option('--url', type=str, prompt='Enter a Zillow URL or select from the default list',
              help='Zillow URL to scrape')
@click.option('--default', is_flag=True, help='Select from the default list of URLs')
@click.option('--price', type=int, default=300000, prompt='Enter starting price', help='Starting price for scraping')
@click.option('--ultra-premium', is_flag=True,
              help='Use ultra premium ScraperAPI (higher costs) only when zillow blocks datacenter IPs')
@click.option('--listing-links-ultra', is_flag=True, help='Use ultra premium for listing links')
@click.option('--test', is_flag=True, help='Run in test mode')
def main(url, default, price, ultra_premium, listing_links_ultra, test):
    """Run the Zillow scraper with specified options."""
    if default:
        state = click.prompt('Select a state', type=click.Choice(ZILLOW_DEFAULT_START_URLS.keys()), show_choices=True)
        url = ZILLOW_DEFAULT_START_URLS[state]

    click.echo(f"Starting Zillow scraper for URL: {url} with starting price ${price}")
    if ultra_premium:
        click.echo("Using ultra premium ScraperAPI")
    if listing_links_ultra:
        click.echo("Using ultra premium for listing links")
    if test:
        click.echo("Running in test mode")

    data = run_scraper(url,
                       starting_price=price,
                       ultra_premium=ultra_premium,
                       listing_links_ultra=listing_links_ultra,
                       test=test)

    click.echo(f"Scraping completed. Total listings scraped: {len(data)}")


if __name__ == "__main__":
    main()
