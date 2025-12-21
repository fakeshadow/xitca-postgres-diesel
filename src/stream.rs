use core::{
    pin::Pin,
    task::{Context, Poll},
};

use diesel::result::Error;
use futures_core::stream::Stream;
use xitca_postgres::RowStreamOwned;

use crate::{error, row::PgRow};

pub struct RowStream {
    stream: RowStreamOwned,
}

impl From<RowStreamOwned> for RowStream {
    fn from(stream: RowStreamOwned) -> Self {
        Self { stream }
    }
}

impl Stream for RowStream {
    type Item = Result<PgRow, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream)
            .poll_next(cx)
            .map_ok(PgRow::new)
            .map_err(error::into_error)
    }
}
