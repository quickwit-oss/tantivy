
mod termdict;
mod streamer;

pub use self::termdict::TermDictionaryImpl;
pub use self::termdict::TermDictionaryBuilderImpl;
pub use self::streamer::TermStreamerImpl;
pub use self::streamer::TermStreamerBuilderImpl;

use schema::FieldType;

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub(crate) enum TermDeserializerOption {
    StrNoPositions,
    StrWithPositions,
    U64,
}

fn make_deserializer_options(field_type: &FieldType) -> TermDeserializerOption {
    match *field_type {
        FieldType::Str(ref text_options) => {
            let indexing_options = text_options.get_indexing_options();
            if indexing_options.is_position_enabled() {
                TermDeserializerOption::StrWithPositions
            }
            else {
                TermDeserializerOption::StrNoPositions
            }
        }
        _ => {
            TermDeserializerOption::U64
        }
    }
}