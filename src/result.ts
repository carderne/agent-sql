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

export type Result<T = void, E = string> = Failure<E> | Success<T>;

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

// These overloads cause problems at callsite because true|false !== boolean
// export function returnOrThrow<T>(result: Result<T>, throws: true): Result<T>;
// export function returnOrThrow<T>(result: Success<T>, throws: false): T;
// export function returnOrThrow(result: Failure<string>, throws: false): never;
export function returnOrThrow<T>(result: Result<T>, throws: boolean): Result<T> | T | never {
  if (!throws) {
    return result;
  }
  if (result.ok) {
    return result.data;
  }
  throw new Error(result.error);
}
