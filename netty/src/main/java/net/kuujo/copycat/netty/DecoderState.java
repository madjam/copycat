package net.kuujo.copycat.netty;

/**
 * State transitions a decoder goes through as it is decoding an incoming message.
 */
public enum DecoderState {
    READ_CONTENT_LENGTH,
    READ_CONTENT,
}