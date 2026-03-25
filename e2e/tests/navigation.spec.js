// @ts-check
const { test, expect } = require('@playwright/test')

const NAV_TABS = [
  { tab: 'dashboard',  title: 'Dashboard' },
  { tab: 'notmdb',     title: 'No TMDB GUID' },
  { tab: 'nomatch',    title: 'TMDB No Match' },
  { tab: 'duplicates', title: 'Multi-Version' },
  { tab: 'franchises', title: 'Franchises' },
  { tab: 'directors',  title: 'Directors' },
  { tab: 'actors',     title: 'Actors' },
  { tab: 'classics',   title: 'Classics' },
  { tab: 'suggestions',title: 'Suggestions' },
  { tab: 'wishlist',   title: 'Wishlist' },
  { tab: 'config',     title: 'Settings' },
  { tab: 'logs',       title: 'Logs' },
]

test.describe('Sidebar navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('all nav buttons are present', async ({ page }) => {
    for (const { tab } of NAV_TABS) {
      await expect(
        page.locator(`button.nav[data-tab="${tab}"]`),
        `nav button for tab "${tab}" should exist`
      ).toBeVisible()
    }
  })

  for (const { tab, title } of NAV_TABS) {
    test(`clicking "${tab}" updates page title to "${title}"`, async ({ page }) => {
      await page.locator(`button.nav[data-tab="${tab}"]`).click()
      await expect(page.locator('#page-title')).toHaveText(title)
    })
  }

  test('active nav item gets .active class', async ({ page }) => {
    await page.locator('button.nav[data-tab="suggestions"]').click()
    await expect(
      page.locator('button.nav[data-tab="suggestions"]')
    ).toHaveClass(/active/)
  })

  test('previous active nav loses .active class on switch', async ({ page }) => {
    // Dashboard is active by default
    await page.locator('button.nav[data-tab="franchises"]').click()
    await expect(
      page.locator('button.nav[data-tab="dashboard"]')
    ).not.toHaveClass(/active/)
  })
})
