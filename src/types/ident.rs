//! Plain SQL identifiers: the names a loader can splice into warehouse SQL unquoted.

/// A plain SQL identifier the load layer can safely interpolate into DDL/COPY
/// without quoting: `[A-Za-z_][A-Za-z0-9_]*`. Round-5: column names are
/// SOURCE-derived and spliced raw into executed warehouse SQL (build_schema,
/// build_copy_select, …), so a name outside this set is an injection vector.
pub(crate) fn is_safe_load_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The Latin letter a Cyrillic letter is drawn identically to, if any.
fn latin_lookalike(c: char) -> Option<char> {
    Some(match c {
        '\u{430}' => 'a',
        '\u{435}' => 'e',
        '\u{43e}' => 'o',
        '\u{440}' => 'p',
        '\u{441}' => 'c',
        '\u{443}' => 'y',
        '\u{445}' => 'x',
        '\u{456}' => 'i',
        '\u{458}' => 'j',
        '\u{455}' => 's',
        '\u{501}' => 'd',
        '\u{4bb}' => 'h',
        '\u{410}' => 'A',
        '\u{412}' => 'B',
        '\u{415}' => 'E',
        '\u{41a}' => 'K',
        '\u{41c}' => 'M',
        '\u{41d}' => 'H',
        '\u{41e}' => 'O',
        '\u{420}' => 'P',
        '\u{421}' => 'C',
        '\u{422}' => 'T',
        '\u{425}' => 'X',
        '\u{406}' => 'I',
        '\u{408}' => 'J',
        '\u{405}' => 'S',
        _ => return None,
    })
}

/// The plain identifier `name` becomes with its Cyrillic look-alikes made Latin; `None` when it needs no fold or no fold makes it plain.
pub(crate) fn latin_fold(name: &str) -> Option<String> {
    if is_safe_load_ident(name) {
        return None;
    }
    let folded: String = name
        .chars()
        .map(|c| latin_lookalike(c).unwrap_or(c))
        .collect();
    is_safe_load_ident(&folded).then_some(folded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_name_that_is_plain_once_its_cyrillic_lookalikes_are_latin_folds() {
        assert_eq!(latin_fold("\u{441}omment").as_deref(), Some("comment"));
        assert_eq!(latin_fold("\u{421}\u{410}\u{422}").as_deref(), Some("CAT"));
        assert_eq!(latin_fold("comment"), None, "a plain name needs no fold");
        assert_eq!(
            latin_fold("\u{438}\u{43c}\u{44f}"),
            None,
            "a Cyrillic word is not a look-alike"
        );
        assert_eq!(latin_fold("\u{441}omment x"), None, "a fold must end plain");
    }

    #[test]
    fn every_look_alike_folds_to_its_latin_twin_and_no_other_letter_does() {
        let cyrillic = "\u{430}\u{435}\u{43e}\u{440}\u{441}\u{443}\u{445}\u{456}\u{458}\u{455}\u{501}\u{4bb}\
                        \u{410}\u{412}\u{415}\u{41a}\u{41c}\u{41d}\u{41e}\u{420}\u{421}\u{422}\u{425}\u{406}\u{408}\u{405}";
        let latin: String = cyrillic.chars().filter_map(latin_lookalike).collect();
        assert_eq!(latin, "aeopcyxijsdhABEKMHOPCTXIJS");
        for c in cyrillic.chars() {
            let name = format!("{c}_1");
            let twin = latin_lookalike(c).expect("a look-alike");
            assert_eq!(latin_fold(&name), Some(format!("{twin}_1")), "{c:?}");
        }
        assert_eq!(latin_lookalike('\u{431}'), None, "a letter with no twin");
        assert_eq!(latin_lookalike('a'), None, "a Latin letter is not folded");
    }
}
