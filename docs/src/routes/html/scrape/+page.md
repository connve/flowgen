# HTML Scrape

Extracts rows from an HTML document with CSS selectors. Each element that matches `row_selector` becomes one row, and each entry in `fields` becomes a column of that row.

## Configuration

```yaml
- html_scrape:
    name: extract_books
    row_selector: "article.product_pod"
    fields:
      title: "h3 a@title"
      url: "h3 a@href"
      price: "p.price_color"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `row_selector` | string | required | CSS selector for the repeating element. One row per match, in document order. |
| `fields` | map | required | Column name to CSS selector. Each selector runs inside the row element. |
| `html_field` | string | | Field of `event.data` that holds the HTML. Without it, `event.data` itself must be the HTML. |
| `emit` | string | `rows` | `rows` emits a JSON array of rows. `wrapped` emits `{"rows": [...]}`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Field selectors

| Selector | Value |
|---|---|
| `p.price` | Text of the first matching element in the row. |
| `a@href` | `href` attribute of the first matching element in the row. |
| `.` | Text of the row element itself. |
| `.@data-id` | `data-id` attribute of the row element itself. |

Text is the text of the element and everything inside it, trimmed. Text from child elements is joined without a separator, so `<p>A<br>B</p>` gives `"AB"`. A missing element, a missing attribute or empty text gives `null`.

Selectors use standard CSS syntax. An invalid selector stops the task at startup.

To pick a column by position, use `:nth-child`. `td:nth-child(2)` is the second cell of a row. `:nth-of-type` counts only siblings with the same tag and applies the class after that, so `p.note:nth-of-type(1)` matches nothing when the first `<p>` in the parent has no `note` class.

## Input

The HTML must be a string, either `event.data` itself or the field named by `html_field`. Set `response_type: text` on the `http_request` that fetches the page.

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [scraper](https://docs.rs/scraper/latest/scraper/) | Array of row objects, or `{"rows": [...]}` with `emit: wrapped`. A page without matching rows gives an empty array. |

Values are strings or `null`. Links keep the value from the page, so relative links stay relative.

```json
[
  {
    "title": "A Light in the Attic",
    "url": "catalogue/a-light-in-the-attic_1000/index.html",
    "price": "£51.77"
  }
]
```

## Examples

**Scrape a page and emit one event per row:**

```yaml
- http_request:
    name: fetch_page
    endpoint: "https://books.toscrape.com/"
    method: GET
    response_type: text

- html_scrape:
    name: extract_books
    row_selector: "article.product_pod"
    fields:
      title: "h3 a@title"
      url: "h3 a@href"
      price: "p.price_color"
      availability: "p.availability"

- iterate:
    name: split_rows
```

**HTML in a field, values read off the row:**

```yaml
- html_scrape:
    name: extract_results
    html_field: body
    row_selector: "table.results tbody tr"
    fields:
      id: ".@data-id"
      name: "td:nth-child(1)"
      date: "td:nth-child(2)"
```
