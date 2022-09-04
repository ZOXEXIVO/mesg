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

    #[inline]
    pub fn convert_i64_to_vec(value: i64) -> IVec {
        IVec::from(value.to_be_bytes().to_vec())
    }
    #[inline]
    pub fn convert_vec_to_i64(vec: IVec) -> i64 {
        i64::from_be_bytes(vec.to_vec().try_into().unwrap())
    }

    #[inline]
    pub fn convert_u64_to_vec(value: u64) -> IVec {
        IVec::from(value.to_be_bytes().to_vec())
    }

    #[inline]
    pub fn convert_u32_to_vec(value: u32) -> IVec {
        IVec::from(value.to_be_bytes().to_vec())
    }
}
