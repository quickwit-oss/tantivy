use super::{id_to_fieldnorm, fieldnorm_to_id};
use directory::ReadOnlySource;
use DocId;

pub struct FieldNormReader {
    data: ReadOnlySource
}

impl FieldNormReader {

    pub fn open(data: ReadOnlySource) -> Self {
        FieldNormReader {
            data
        }
    }

    pub fn fieldnorm(&self, doc_id: DocId) -> u32 {
        let fieldnorm_id = self.fieldnorm_id(doc_id);
        id_to_fieldnorm(fieldnorm_id)
    }

    #[inline(always)]
    pub fn fieldnorm_id(&self, doc_id: DocId) -> u8 {
        let fielnorms_data = self.data.as_slice();
        fielnorms_data[doc_id as usize]
    }

    #[inline(always)]
    pub fn id_to_fieldnorm(id: u8) -> u32 {
        id_to_fieldnorm(id)
    }

    #[inline(always)]
    pub fn fieldnorm_to_id(fieldnorm: u32) -> u8 {
        fieldnorm_to_id(fieldnorm)
    }
}

#[cfg(test)]
impl From<Vec<u32>> for FieldNormReader {
    fn from(field_norms: Vec<u32>) -> FieldNormReader {
        let field_norms_id = field_norms.into_iter()
            .map(FieldNormReader::fieldnorm_to_id)
            .collect::<Vec<u8>>();
        let field_norms_data = ReadOnlySource::from(field_norms_id);
        FieldNormReader {
            data: field_norms_data
        }
    }
}