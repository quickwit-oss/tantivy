mod vector_writer;
mod vector_reader;

pub use self::vector_writer::{VectorWriter, VectorWriters};
pub use self::vector_reader::{VectorReader, VectorReaders};


#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::schema::Field;

    use super::{VectorReaders, VectorWriters};


    #[test]
    fn test_vector_reader_writer() -> crate::Result<()> {
        let vectors_path = Path::new("vectors_test").to_path_buf();
        let field = Field::from_field_id(1);

        {
            let mut vws = VectorWriters::new(&vectors_path);
            let v1: Vec<f32> = vec![0.1,0.1,0.1];
            let v2: Vec<f32> = vec![0.2,0.2,0.2];
            let v3: Vec<f32> = vec![0.3,0.3,0.3];

            vws.record(1, field, &v1)?;
            vws.record(2, field, &v2)?;
            vws.record(3, field, &v3)?;
        }
        
        let mut vrs = VectorReaders::new(&vectors_path);
        let vr1 = vrs.open_read(field);
        let v = vr1.vector(1).unwrap();
        dbg!(v);

        let query: Vec<f32> = vec![1.0,1.0,1.0];
        let res = vr1.search(&query, 50);

        dbg!(res);

        Ok(())
    }
}