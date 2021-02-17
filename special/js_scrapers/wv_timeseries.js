const { chromium } = require('playwright');
const fastcsv = require('fast-csv');
const { DateTime } = require("luxon");

// scrape WV dashboard for

(async (callbackfn, thisArg) => {
  const URL = 'https://app.powerbigov.us/view?r=eyJrIjoiOGVkMjJmN2EtMTA5My00MTM0LWExZWUtMTUzMjkwOGNkYjlhIiwidCI6IjhhMjZjZjAyLTQzNGEtNDMxZS04Y2FkLTdlYWVmOTdlZjQ4NCJ9'
  // some setup
  const browser = await chromium.launch({
    headless: false
  })
  const context = await browser.newContext()
  const page = await context.newPage()

  await page.goto(URL)

  // click on the correct tab and get the data table
  await page.click('//button[normalize-space(.)=\'Case and Lab Trends\']')
  await page.click('text="Daily Lab Test"', {button: 'right'})
  await page.click('text="Show as a table"')
  await page.waitForSelector('text="Lab Report Date"')
  await page.waitForTimeout(2000)

  // ok let's go. we'll grab all the visible data, add it to `data`, scroll down, and keep doing that until
  // no new data is emerging, which means we've seen every cell
  let data = {}
  let lastDataSize = -1
  while (Object.keys(data).length  != lastDataSize) {
    lastDataSize = Object.keys(data).length

    const dataCells = await page.$$('.bodyCells .cell-interactive')
    // hover over each cell and extract data from the tooltip
    for (const dataCell of dataCells) {
      if (await dataCell.isHidden()) continue // skip hidden cells
      await dataCell.scrollIntoViewIfNeeded() // the tooltip doesn't appear unless the cell is visible
      await dataCell.hover()
      const tooltipElem = await page.waitForSelector('.tooltip-container')
      const date = await (await tooltipElem.$('.tooltip-row:nth-child(1) .tooltip-value-cell div')).textContent()
      const dataType = await (await tooltipElem.$('.tooltip-row:nth-child(2) .tooltip-title-cell div')).textContent()
      const value = await (await tooltipElem.$('.tooltip-row:nth-child(2) .tooltip-value-cell div')).textContent()
      if (data[date] === undefined) data[date] = {}
      data[date][dataType] = value
    }

    // click the down scroll button a bunch of times to get more data
    const elemScrollDown = await page.$('.scroll-bar-div:not([style*="visibility: hidden"]) .scroll-bar-part-arrow:nth-child(2)')
    for (const i of Array.from(Array(15))) {
      await elemScrollDown.click()
    }
    await page.waitForTimeout(2000)
  }

  // output all the data to stdout in csv format
  const csvStream = fastcsv.format({ headers: true })
  csvStream.pipe(process.stdout).on('end', () => process.exit());
  for (const date of Object.keys(data)) {
    const formattedDate = DateTime.fromFormat(date.trim(), "M/d/yy").toFormat('yyyyMMdd')
    csvStream.write({date: formattedDate, ...data[date]})
  }
  csvStream.end();

  // cleanup
  await page.close();
  await context.close();
  await browser.close();
})().catch((ex) => {console.error(ex); process.exit(1)});
