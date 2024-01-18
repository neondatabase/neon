// https://docs.rs/axum-core/0.4.3/src/axum_core/body.rs.html

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{TryStream, TryStreamExt};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, BoxError>;

fn boxed<B>(body: B) -> BoxBody
where
    B: http_body::Body<Data = Bytes> + Sync + Send + 'static,
    B::Error: Into<BoxError>,
{
    try_downcast(body).unwrap_or_else(|body| body.map_err(|x| x.into()).boxed())
}

pub(crate) fn try_downcast<T, K>(k: K) -> Result<T, K>
where
    T: 'static,
    K: Send + 'static,
{
    let mut k = Some(k);
    if let Some(k) = <dyn std::any::Any>::downcast_mut::<Option<T>>(&mut k) {
        Ok(k.take().unwrap())
    } else {
        Err(k.unwrap())
    }
}

/// The body type used in axum requests and responses.
#[derive(Debug)]
pub struct Body(BoxBody);

impl Body {
    /// Create a new `Body` that wraps another [`http_body::Body`].
    pub fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Sync + Send + 'static,
        B::Error: Into<BoxError>,
    {
        try_downcast(body).unwrap_or_else(|body| Self(boxed(body)))
    }

    /// Create an empty body.
    pub fn empty() -> Self {
        Self::new(http_body_util::Empty::new())
    }

    /// Create a new `Body` from a [`Stream`].
    ///
    /// [`Stream`]: https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: TryStream + Sync + Send + 'static,
        S::Ok: Into<Bytes>,
        S::Error: Into<BoxError>,
    {
        Self::new(StreamBody::new(
            stream.map_ok(|chunk| Frame::data(chunk.into())),
        ))
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<()> for Body {
    fn from(_: ()) -> Self {
        Self::empty()
    }
}

macro_rules! body_from_impl {
    ($ty:ty) => {
        impl From<$ty> for Body {
            fn from(buf: $ty) -> Self {
                Self::new(http_body_util::Full::from(buf))
            }
        }
    };
}

body_from_impl!(&'static [u8]);
body_from_impl!(std::borrow::Cow<'static, [u8]>);
body_from_impl!(Vec<u8>);

body_from_impl!(&'static str);
body_from_impl!(std::borrow::Cow<'static, str>);
body_from_impl!(String);

body_from_impl!(Bytes);

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = BoxError;

    #[inline]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.0).poll_frame(cx)
    }

    #[inline]
    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }
}
