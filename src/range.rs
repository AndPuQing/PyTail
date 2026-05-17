#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeParseError;

pub fn parse_byte_range(value: &str, size: u64) -> Result<Option<ByteRange>, RangeParseError> {
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Err(RangeParseError);
    }

    let (start, end) = spec.split_once('-').ok_or(RangeParseError)?;
    if start.is_empty() {
        let suffix_len = end.parse::<u64>().map_err(|_| RangeParseError)?;
        if suffix_len == 0 || size == 0 {
            return Err(RangeParseError);
        }
        let start = size.saturating_sub(suffix_len);
        return Ok(Some(ByteRange {
            start,
            end: size - 1,
        }));
    }

    let start = start.parse::<u64>().map_err(|_| RangeParseError)?;
    if start >= size {
        return Err(RangeParseError);
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        let end = end.parse::<u64>().map_err(|_| RangeParseError)?;
        if start > end {
            return Err(RangeParseError);
        }
        end.min(size - 1)
    };
    Ok(Some(ByteRange { start, end }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_byte_ranges() {
        assert_eq!(
            parse_byte_range("bytes=2-5", 10),
            Ok(Some(ByteRange { start: 2, end: 5 }))
        );
        assert_eq!(
            parse_byte_range("bytes=6-", 10),
            Ok(Some(ByteRange { start: 6, end: 9 }))
        );
        assert_eq!(
            parse_byte_range("bytes=-4", 10),
            Ok(Some(ByteRange { start: 6, end: 9 }))
        );
    }

    #[test]
    fn rejects_invalid_or_unsatisfiable_ranges() {
        assert_eq!(parse_byte_range("bytes=10-", 10), Err(RangeParseError));
        assert_eq!(parse_byte_range("bytes=6-4", 10), Err(RangeParseError));
        assert_eq!(parse_byte_range("bytes=-0", 10), Err(RangeParseError));
        assert_eq!(parse_byte_range("bytes=0-1,2-3", 10), Err(RangeParseError));
    }
}
