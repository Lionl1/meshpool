//! Counts words in a text. Input: a JSON string; output: a JSON object word -> count, in
//! sorted order (a `BTreeMap`, so every replica writes the same bytes).

use std::collections::BTreeMap;

fn wordcount(text: String) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for word in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        *counts.entry(word.to_lowercase()).or_insert(0) += 1;
    }
    counts
}

meshpool_task::task_json!(wordcount);

#[test]
fn works() {
    let counts = wordcount("To be, or not to be".into());
    assert_eq!(counts["to"], 2);
    assert_eq!(counts.len(), 4);
}
