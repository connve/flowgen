//! The docs pages of this version, embedded as Markdown, so agents read the
//! fields and examples of the tasks this binary runs, also offline.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::Json;
use flowgen_client::types as api;
use rust_embed::RustEmbed;

/// The docs site's routes, each page a [`PAGE_FILE`].
#[derive(RustEmbed)]
#[folder = "../../docs/src/routes/"]
struct Sources;

/// The file of a docs page in its route folder.
const PAGE_FILE: &str = "+page.md";

/// What a diagram starts with: `<Mermaid chart={`…`} />`.
const MERMAID_OPEN: &str = "<Mermaid chart={`";

/// The docs pages, by path.
pub async fn list_docs() -> Json<api::DocPages> {
    Json(api::DocPages { pages: index() })
}

/// One docs page as Markdown.
pub async fn read_doc(Path(page): Path<String>) -> Result<Json<api::DocContent>, StatusCode> {
    match read(&page) {
        Some(content) => Ok(Json(api::DocContent { page, content })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// The docs pages, by path such as `object-store/object-store`.
pub fn index() -> Vec<api::DocPage> {
    let mut pages: Vec<api::DocPage> = Sources::iter()
        .filter_map(|file| {
            let page = file.strip_suffix(PAGE_FILE)?.trim_end_matches('/');
            let markdown = read(page)?;
            let title = match title(&markdown) {
                Some(title) => title,
                None => page,
            };
            Some(api::DocPage {
                title: title.to_string(),
                summary: summary(&markdown),
                page: page.to_string(),
            })
        })
        .collect();
    pages.sort_by(|a, b| a.page.cmp(&b.page));
    pages
}

/// The page as plain Markdown, as the docs site's `/md/` serves it.
pub fn read(page: &str) -> Option<String> {
    if page
        .split('/')
        .any(|segment| segment.is_empty() || segment == "..")
    {
        return None;
    }
    let source = Sources::get(&format!("{page}/{PAGE_FILE}"))?;
    Some(plain(&String::from_utf8_lossy(&source.data)))
}

/// `source` without its frontmatter and Svelte imports, and with its diagrams as
/// Mermaid code blocks.
fn plain(source: &str) -> String {
    let source = match source
        .strip_prefix("---\n")
        .and_then(|rest| rest.split_once("\n---\n"))
    {
        Some((_, rest)) => rest.trim_start(),
        None => source,
    };
    let body = match source
        .strip_prefix("<script>")
        .and_then(|rest| rest.split_once("</script>"))
    {
        Some((_, rest)) => rest,
        None => source,
    };
    let mut markdown = String::new();
    let mut rest = body;
    while let Some(start) = rest.find(MERMAID_OPEN) {
        let chart = &rest[start + MERMAID_OPEN.len()..];
        let Some((diagram, after)) = chart.split_once("`}") else {
            break;
        };
        let Some((_, after)) = after.split_once("/>") else {
            break;
        };
        markdown.push_str(&rest[..start]);
        markdown.push_str(&format!("```mermaid\n{}\n```", diagram.trim()));
        rest = after;
    }
    markdown.push_str(rest);
    markdown.trim().to_string()
}

/// The page's first `# ` heading.
fn title(markdown: &str) -> Option<&str> {
    markdown
        .lines()
        .find_map(|line| line.strip_prefix("# "))
        .map(str::trim)
}

/// The first sentence of the page's first paragraph that ends a sentence or
/// leads into a list, so a lone "or" between two code blocks doesn't count.
fn summary(markdown: &str) -> String {
    let Some(paragraph) = markdown.split("\n\n").map(str::trim).find(|block| {
        block.ends_with(['.', '!', '?', ':'])
            && !block.starts_with("```")
            && !block.starts_with(['#', '|', '-', '*', '>', '<'])
    }) else {
        return String::new();
    };
    let text = paragraph.split_whitespace().collect::<Vec<_>>().join(" ");
    let end = text.char_indices().find(|&(index, c)| {
        matches!(c, '.' | '!' | '?')
            && match text[index + 1..].strip_prefix(' ') {
                Some(next) => next.starts_with(|c: char| c.is_uppercase() || c == '`' || c == '('),
                None => index + 1 == text.len(),
            }
    });
    match (end, text.strip_suffix(':')) {
        (Some((index, _)), _) => text[..=index].to_string(),
        (None, Some(lead)) => format!("{lead}."),
        (None, None) => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_reads_as_plain_markdown() {
        let source = "<script>\n  import Mermaid from 'a';\n</script>\n\n# A\n\nOne. Two.\n\n<Mermaid chart={`\ngraph LR\n  a --> b\n`} />\n\nEnd.";

        let markdown = plain(source);

        assert_eq!(
            markdown,
            "# A\n\nOne. Two.\n\n```mermaid\ngraph LR\n  a --> b\n```\n\nEnd."
        );
        assert_eq!(title(&markdown), Some("A"));
        assert_eq!(summary(&markdown), "One.");
    }

    #[test]
    fn the_frontmatter_and_paragraphs_without_a_sentence_are_left_out() {
        let source = "---\ndescription: For search.\n---\n\n# A\n\n```sh\na\n```\n\nor\n\n```sh\nb\n```\n\nOne. Two.";

        let markdown = plain(source);

        assert!(markdown.starts_with("# A"), "{markdown}");
        assert_eq!(summary(&markdown), "One.");
        assert_eq!(
            summary("# A\n\nSplits signals:\n\n- one"),
            "Splits signals."
        );
        assert_eq!(
            summary("# A\n\nReads keys. The url picks:\n\n| a |"),
            "Reads keys."
        );
    }

    #[test]
    fn the_embedded_pages_are_listed_and_read() {
        let pages = index();
        let page = pages
            .iter()
            .find(|page| page.page == "object-store/object-store")
            .unwrap();

        assert!(!page.title.is_empty());
        assert!(read(&page.page).unwrap().contains("credentials_path"));
        assert_eq!(read("../object-store"), None);
        assert_eq!(read("object-store/missing"), None);
    }
}
