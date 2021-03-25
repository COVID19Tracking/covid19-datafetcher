const { chromium } = require('playwright');
const fastcsv  = require('fast-csv');
const retry = require('async-retry');

(async (callbackfn, thisArg) => {
  const URL = 'https://scdhec.gov/covid19/south-carolina-county-level-data-covid-19'  // some setup
  const browser = await chromium.launch({
    headless: true
  })
  const context = await browser.newContext({
    viewport: { width: 2280, height: 1024 }
  })

  const fetchTableData = async () => {
    const page = await context.newPage()

    // load page, look for the Tableau iframe, click the testing button
    await page.goto(URL)
    await page.waitForSelector('iframe.tableauViz')
    await page.waitForTimeout(10000)
    const frame = await page.frame({url: /.*tableau.*/})
    await frame.click('div[role="button"]:has-text("TESTING")')

    const tableElement = await frame.waitForSelector("div[tb-test-id='Testing Table'] [aria-label='Data Visualization'] img")

    const tableBox = await tableElement.boundingBox();
    const x_max = tableBox.width - 20
    const y_max = tableBox.height - 20
    const data = {}
    // turns out the data table is actually an image. This is a garbage way of doing it, but just hover over parts
    // of the table image in the hope a tooltip pops up and scrape out the contents of the tooltips
    for (let x = 30; x < x_max; x+= 85) {
      for (let y = 15; y < y_max; y+= 10) {
        await tableElement.hover({position: {x: x, y: y}})
        await page.waitForTimeout(3000) // wait for tooltip

        // find the 3 data cells from the tooltip (row header, column header, value)
        let cellData = await frame.$$("div.tab-ubertipTooltip table td:nth-child(3)")
        // extract the content from each cell
        for (let i=0; i<cellData.length; i++) {
          cellData[i] = await cellData[i].textContent()
        }

        // save the data
        if (data[cellData[0]] == undefined) {
          data[cellData[0]] = {}
        }
        data[cellData[0]][cellData[1]] = cellData[2]
      }
    }

    await page.close();

    // check to make sure we got all the data
    if (Object.keys(data).length != 6) { throw Error("not enough rows") }
    for (const key of Object.keys(data)) {
      if (key == "undefined" ) { continue }
      if (Object.keys(data[key]).length < 3) { throw Error("not enough columns") }
    }
    return data
  }

  // we'll try a few times in case one attempt goes wrong
  const data = await retry(fetchTableData, { retries: 5 } )

  // output all the data to stdout in csv format
  const csvStream = fastcsv.format({ headers: true })
  csvStream.pipe(process.stdout).on('end', () => process.exit());
  for (const key of Object.keys(data)) {
    if (key == "undefined" ) { continue }
    csvStream.write({"Type": key, ...data[key]})
  }
  csvStream.end();

  // cleanup
  await context.close();
  await browser.close();
})().catch((ex) => {console.error(ex); process.exit(1)});
