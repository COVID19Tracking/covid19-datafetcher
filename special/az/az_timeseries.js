const { chromium } = require('playwright');
const { DateTime } = require("luxon");
const yargs = require('yargs');
const retry = require('async-retry');
const fastcsv  = require('fast-csv');

(async (callbackfn, thisArg) => {
  const argv = yargs
    .scriptName("az_timeseries")
    .usage('$0 <cmd>')
    .command('diagnostic', 'Scrapes diagnostic data')
    .command('serology', 'Scrapes serology data')
    .help()
    .demandCommand(1)
    .strict()
    .argv

  // get the desired command and map it to a selection to make from the Tableau menu
  const command = argv._[0]
  const commandMapping = {"diagnostic": "Diagnostic", "serology": "Serology"}


  const URL = 'https://tableau.azdhs.gov/views/ELRv2testlevelandpeopletested/TestsConducted?%3Aembed=y&%3AshowVizHome=no&%3Ahost_url=https%3A%2F%2Ftableau.azdhs.gov%2F&%3Aembed_code_version=3&%3Atabs=yes&%3Atoolbar=no&%3AshowAppBanner=false&%3Adisplay_spinner=no&%3AloadOrderID=3'
  // some setup
  const browser = await chromium.launch({
    headless: true
  })
  const context = await browser.newContext({
    recordVideo: { dir: 'videos/' },
    viewport: { width: 2280, height: 1024 } // a nice wide viewport helps ensure we don't skip any days
  })
  const page = await context.newPage()

  await page.goto(URL)

  // choose test type
  // TODO: do both diagnostic and antibody tests
  await page.click('//div[1][normalize-space(.)=\'All\']')
  await page.click('//div[normalize-space(@role)=\'menuitem\']/div[normalize-space(.)=\''+commandMapping[command]+'\']')

  // find the chart with the time series data
  const chart_selector = '//div[starts-with(normalize-space(.), \''+commandMapping[command]+' tests by date of collection Press ESC to clear any mark selections. A\')]/div[11][normalize-space(@role)=\'img\']/div[1]/div[2]/canvas[2]'
  await page.waitForSelector(chart_selector)
  await page.waitForTimeout(1000)
  const chart = await page.$(chart_selector)
  const chartbox = await chart.boundingBox()

  // position the mouse at the bottom left of the box, then move it to the right pixel by pixel
  const fetchChartData = async (bail, attempt) => {
    let x, lastDate;
    const data = {}
    for (x = 2; x < chartbox.width - 48; x++) {
      // it seems to work best to overshoot the target and then click on it, to avoid gaps
      await page.mouse.move(chartbox.x + 2 + x + 1, chartbox.y + chartbox.height - 2)
      await page.mouse.click(chartbox.x + 2 + x, chartbox.y + chartbox.height - 2)
      await page.waitForTimeout(300)

      // find the tooltip and extract the date and value from it
      await page.waitForSelector('body > div.tab-tooltip')
      const dateElem = await page.$('body > div.tab-tooltip .tab-ubertipContent >> text=Date >> xpath=following-sibling::*[1]')
      const date = await dateElem.innerText()
      const valueElem = await page.$('body > div.tab-tooltip .tab-ubertipContent >> text=Number >> xpath=following-sibling::*[1]')
      const value = await valueElem.innerText()
      data[date.trim()] = value.trim().replace(/,/g, '')

      // check to confirm that we don't have any gaps in the time series
      // the current date should be either the same as the previous date (duplicates are fine and expected)
      // or one day more than the previous date
      const curDate = DateTime.fromFormat(date.trim(), "MMMM d, yyyy")
      if (lastDate !== undefined && !(curDate.equals(lastDate) || curDate.equals(lastDate.plus({days: 1})))) {
        throw new Error(`Gap in dates ${lastDate}—${curDate}`)
      }
      lastDate = curDate
    }
    return data
  }

  // if there are errors, we'll retry a few times in the hope of getting a complete time series
  // if all the attempts fail, we'll exit with an error
  const data = await retry(fetchChartData, { retries: 3 } )

  // output all the data to stdout in csv format
  const csvStream = fastcsv.format({ headers: true })
  csvStream.pipe(process.stdout).on('end', () => process.exit());
  for (const date of Object.keys(data)) {
    csvStream.write({date: date, value: data[date]})
  }
  csvStream.end();

  // cleanup
  await page.close();
  await context.close();
  await browser.close();
})().catch((ex) => {console.error(ex); process.exit(1)});
