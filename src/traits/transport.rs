use crate::message::Envelope;

pub trait Transport<C, S> {
    fn send(&self, msg: Envelope<C, S>);
    fn send_batch(&self, msg: Vec<Envelope<C, S>>);
}
