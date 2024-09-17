use core::{num::NonZeroU32, ops::Range};

use std::error;

use diesel::{
    backend::Backend,
    pg::{Pg, PgValue, TypeOidLookup},
    row::{Field, PartialRow, Row, RowIndex, RowSealed},
};
use xitca_postgres::{
    compat::RowOwned,
    types::{FromSql, Type},
};

pub struct PgRow {
    row: RowOwned,
}

impl PgRow {
    pub(super) fn new(row: RowOwned) -> Self {
        Self { row }
    }
}
impl RowSealed for PgRow {}

impl<'a> Row<'a, Pg> for PgRow {
    type Field<'b> = PgField<'b> where Self: 'b, 'a: 'b;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.row.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(PgField {
            row: &self.row,
            idx,
        })
    }

    fn partial_row(&self, range: Range<usize>) -> PartialRow<Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for PgRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.row.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a> RowIndex<&'a str> for PgRow {
    fn idx(&self, idx: &'a str) -> Option<usize> {
        self.row.columns().iter().position(|c| c.name() == idx)
    }
}

pub struct PgField<'a> {
    row: &'a RowOwned,
    idx: usize,
}

impl<'a> Field<'a, Pg> for PgField<'a> {
    fn field_name(&self) -> Option<&str> {
        Some(self.row.columns()[self.idx].name())
    }

    fn value(&self) -> Option<<Pg as Backend>::RawValue<'_>> {
        let DieselFromSqlWrapper(value) = self.row.get_raw(self.idx);
        value
    }
}

struct TyWrapper<'a>(&'a Type);

impl TypeOidLookup for TyWrapper<'_> {
    fn lookup(&self) -> NonZeroU32 {
        NonZeroU32::new(self.0.oid()).unwrap()
    }
}

struct DieselFromSqlWrapper<'a>(Option<PgValue<'a>>);

impl<'a> FromSql<'a> for DieselFromSqlWrapper<'a> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn error::Error + Send + Sync>> {
        let ty = unsafe { &*(ty as *const Type as *const TyWrapper) };
        Ok(DieselFromSqlWrapper(Some(PgValue::new(raw, ty))))
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn error::Error + Sync + Send>> {
        Ok(DieselFromSqlWrapper(None))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() != 0
    }
}
