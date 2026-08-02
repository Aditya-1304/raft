use crate::message::Envelope;

pub trait Transport<C, S> {
    type Error;

    fn send(&self, msg: Envelope<C, S>) -> Result<(), Self::Error>;
    fn send_batch(&self, msg: Vec<Envelope<C, S>>) -> Result<(), Self::Error>;
}
