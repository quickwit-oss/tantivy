use std::ops::{Deref, DerefMut};

pub(crate) struct MutableEnum<T>(Option<T>);

impl<T> MutableEnum<T> {
    pub fn wrap(val: T) -> Self {
        MutableEnum(Some(val))
    }

    pub fn into(self) -> T {
        self.0.unwrap()
    }
}

impl<T> MutableEnum<T> {
    pub fn map_mutate<E, F>(&mut self, transformation: F) -> Result<(), E>
    where
        F: FnOnce(T) -> Result<T, E>,
    {
        self.0 = self.0.take().map(transformation).transpose()?;
        Ok(())
    }
}

impl<T> Deref for MutableEnum<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl<T> DerefMut for MutableEnum<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}
