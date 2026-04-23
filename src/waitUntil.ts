const DEFAULT_POLL_MS = 10

export type WaitUntilOptions = {
    /** Max wait in ms. Omit or `Infinity` for no limit. */
    timeout?: number
    pollMs?: number
}

/**
 * Poll until `predicate` returns true. Matches prior async-wait-until usage:
 * second argument may be a timeout in ms or `{ timeout, pollMs }`.
 */
export async function waitUntil(
    predicate: () => boolean | undefined | void,
    timeoutOrOptions?: number | WaitUntilOptions
): Promise<void> {
    let timeout = Infinity
    let pollMs = DEFAULT_POLL_MS
    if (typeof timeoutOrOptions === 'number') {
        timeout = timeoutOrOptions
    } else if (timeoutOrOptions !== undefined) {
        if (timeoutOrOptions.timeout !== undefined) {
            timeout = timeoutOrOptions.timeout
        }
        if (timeoutOrOptions.pollMs !== undefined) {
            pollMs = timeoutOrOptions.pollMs
        }
    }

    const start = Date.now()
    for (;;) {
        if (predicate() === true) {
            return
        }
        if (Number.isFinite(timeout) && Date.now() - start >= timeout) {
            throw new Error('waitUntil timed out')
        }
        await new Promise<void>((resolve) => setTimeout(resolve, pollMs))
    }
}
