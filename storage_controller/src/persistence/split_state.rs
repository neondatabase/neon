use diesel::pg::{Pg, PgValue};
use diesel::{
    deserialize::FromSql, deserialize::FromSqlRow, expression::AsExpression, serialize::ToSql,
    sql_types::Int2,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, FromSqlRow, AsExpression)]
#[diesel(sql_type = SplitStateSQLRepr)]
#[derive(Deserialize, Serialize)]
pub enum SplitState {
    Idle = 0,
    Splitting = 1,
}

impl Default for SplitState {
    fn default() -> Self {
        Self::Idle
    }
}

type SplitStateSQLRepr = Int2;

impl ToSql<SplitStateSQLRepr, Pg> for SplitState {
    fn to_sql<'a>(
        &'a self,
        out: &'a mut diesel::serialize::Output<Pg>,
    ) -> diesel::serialize::Result {
        let raw_value: i16 = *self as i16;
        let mut new_out = out.reborrow();
        ToSql::<SplitStateSQLRepr, Pg>::to_sql(&raw_value, &mut new_out)
    }
}

impl FromSql<SplitStateSQLRepr, Pg> for SplitState {
    fn from_sql(pg_value: PgValue) -> diesel::deserialize::Result<Self> {
        match FromSql::<SplitStateSQLRepr, Pg>::from_sql(pg_value).map(|v| match v {
            0 => Some(Self::Idle),
            1 => Some(Self::Splitting),
            _ => None,
        })? {
            Some(v) => Ok(v),
            None => Err(format!("Invalid SplitState value, was: {:?}", pg_value.as_bytes()).into()),
        }
    }
}
