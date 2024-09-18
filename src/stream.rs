use core::{
    pin::Pin,
    task::{Context, Poll},
};

use diesel::result::Error;
use futures_core::stream::Stream;
use xitca_postgres::compat::RowStreamOwned;

use crate::{error, row::PgRow};

pub struct RowStream {
    stream: RowStreamOwned,
}

impl From<xitca_postgres::RowStream<'_>> for RowStream {
    fn from(stream: xitca_postgres::RowStream<'_>) -> Self {
        Self {
            stream: RowStreamOwned::from(stream),
        }
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
