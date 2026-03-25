export type Failure<E = unknown> = {
  ok: false;
  error: E;
  unwrap(): never;
};

export type Success<T = void> = {
  ok: true;
  data: T;
  unwrap(): T;
};

export type Result<T = void, E = Error> = Failure<E> | Success<T>;

export function Err<E>(error: E): Failure<E> {
  return {
    ok: false,
    error,
    unwrap() {
      throw new Error(String(error));
    },
  };
}

export function Ok<T>(data: T): Success<T> {
  return {
    ok: true,
    data,
    unwrap() {
      return data;
    },
  };
}
