use sled::IVec;

pub struct IdPair {
    pub vec: IVec,
    pub value: u64,
}

impl IdPair {
    pub fn new(value: u64, vec: IVec) -> Self {
        IdPair { vec, value }
    }

    pub fn from_value(value: u64) -> Self {
        IdPair {
            value,
            vec: IVec::from(&value.to_be_bytes()),
        }
    }

    pub fn from_vec(vec: IVec) -> Self {
        IdPair { value: 0, vec }
    }

    pub fn convert_i64_to_vec(value: i64) -> IVec {
        IVec::from(value.to_be_bytes().to_vec())
    }

    pub fn convert_u64_to_vec(value: u64) -> IVec {
        IVec::from(value.to_be_bytes().to_vec())
    }
}
