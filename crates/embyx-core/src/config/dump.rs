//! `BaseModel.model_dump_json()`: the bytes a section is stored as.
//!
//! pydantic writes compact JSON (no spaces), keeps non-ASCII text as is (`ensure_ascii`
//! off, so DEL and U+2028 stay raw), escapes control characters like `json.dumps`, writes
//! a float as Python's `repr` with the exponent's leading zeros removed (`1e-7`, `1e+16`),
//! except that the decade below 1e-4 stays in fixed notation (`0.00001`), and a non-finite
//! float as `null`.

use crate::pyjson::{DumpOptions, Value, dumps, float_repr};

/// The stored bytes of a dumped model.
pub fn pydantic_json(value: &Value) -> String {
    let mut out = String::new();
    write(&mut out, value);
    out
}

fn write(out: &mut String, value: &Value) {
    match value {
        Value::Float(float) => out.push_str(&float_text(*float)),
        Value::List(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write(out, item);
            }
            out.push(']');
        }
        Value::Dict(dict) => {
            out.push('{');
            for (index, (key, item)) in dict.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                // A dumped model has only field names (and `dict[str, ...]` keys) as keys.
                out.push_str(&scalar(&Value::Str(key.as_str().unwrap_or_default().to_owned())));
                out.push(':');
                write(out, item);
            }
            out.push('}');
        }
        other => out.push_str(&scalar(other)),
    }
}

fn scalar(value: &Value) -> String {
    dumps(value, &DumpOptions::compact()).expect("a scalar always dumps")
}

/// A float as pydantic writes it.
pub fn float_text(value: f64) -> String {
    if !value.is_finite() {
        return "null".to_owned();
    }
    let repr = float_repr(value);
    match repr.split_once('e') {
        // pydantic switches to scientific notation one decade later than `repr` on the
        // small side: 1e-05 is written 0.00001, 1e-06 stays 1e-6.
        Some((mantissa, "-05")) => {
            let (sign, mantissa) = mantissa.strip_prefix('-').map_or(("", mantissa), |rest| ("-", rest));
            format!("{sign}0.0000{}", mantissa.replace('.', ""))
        }
        Some((mantissa, exponent)) => {
            let (sign, digits) = exponent.split_at(1);
            format!("{mantissa}e{sign}{}", digits.trim_start_matches('0'))
        }
        None => repr,
    }
}

#[cfg(test)]
mod tests {
    use super::{float_text, pydantic_json};
    use crate::pyjson::{Dict, Value};

    #[test]
    fn floats_drop_exponent_zeros_and_non_finite_is_null() {
        assert_eq!(float_text(1e-7), "1e-7");
        assert_eq!(float_text(1e16), "1e+16");
        assert_eq!(float_text(0.5), "0.5");
        assert_eq!(float_text(1e-5), "0.00001");
        assert_eq!(float_text(-9.5e-5), "-0.000095");
        assert_eq!(float_text(1e-6), "1e-6");
        assert_eq!(float_text(f64::NAN), "null");
        assert_eq!(float_text(f64::NEG_INFINITY), "null");
    }

    #[test]
    fn writes_compact_unicode_json() {
        let mut dict = Dict::new();
        dict.insert("a", Value::Str("中\u{1}\u{7f}\u{2028}".to_owned()));
        dict.insert("b", Value::List(vec![Value::Int(1), Value::Bool(true), Value::Null]));
        assert_eq!(
            pydantic_json(&Value::Dict(dict)),
            "{\"a\":\"中\\u0001\u{7f}\u{2028}\",\"b\":[1,true,null]}"
        );
    }
}
