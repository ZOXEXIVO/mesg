use sled::IVec;

pub struct IdPair {
    vector: Option<IVec>,
    value: Option<u64>,
}

impl IdPair {
    pub fn value(&self) -> u64 {
        if let Some(val) = self.value {
            return val;
        }

        if let Some(vec) = &self.vector {
            return u64::from_be_bytes(vec.to_vec().try_into().unwrap());
        }

        panic!("invalid id-pair");
    }

    pub fn vector(&self) -> IVec {
        if let Some(vec) = &self.vector {
            return vec.to_owned();
        }

        if let Some(val) = self.value {
            return IVec::from(&val.to_be_bytes());
        }

        panic!("invalid id-pair");
    }

    pub fn from_value(value: u64) -> Self {
        IdPair {
            vector: None,
            value: Some(value),
        }
    }

    pub fn from_vector(vector: IVec) -> Self {
        IdPair {
            vector: Some(vector),
            value: None,
        }
    }
}

// Converters

impl From<IdPair> for IVec {
    fn from(value: IdPair) -> Self {
        value.vector()
    }
}

impl From<&IdPair> for IVec {
    fn from(value: &IdPair) -> Self {
        value.vector()
    }
}

impl From<&IdPair> for i64 {
    fn from(id: &IdPair) -> Self {
        id.value() as i64
    }
}

impl From<IdPair> for i64 {
    fn from(value: IdPair) -> Self {
        value.value() as i64
    }
}

impl From<&IdPair> for u64 {
    fn from(id: &IdPair) -> Self {
        id.value()
    }
}

impl From<IdPair> for u64 {
    fn from(value: IdPair) -> Self {
        value.value()
    }
}

impl From<i64> for IdPair {
    fn from(value: i64) -> Self {
        IdPair::from_vector(IVec::from(value.to_be_bytes().to_vec()))
    }
}

impl From<u64> for IdPair {
    fn from(value: u64) -> Self {
        IdPair::from_vector(IVec::from(value.to_be_bytes().to_vec()))
    }
}

impl From<u32> for IdPair {
    fn from(value: u32) -> Self {
        IdPair::from_vector(IVec::from(value.to_be_bytes().to_vec()))
    }
}
