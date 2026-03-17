use crate::message::Envelope;

pub trait Transport<C> {
  fn send(&self, msg: Envelope<C>);
  fn send_batch(&self, msg: Vec<Envelope<C>>);
}