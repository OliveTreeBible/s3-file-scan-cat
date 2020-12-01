export class EmptyPrefixError extends Error {
    constructor() {
        super('Failed to get common prefixes for directory listing in S3.')
        this.name = 'EmptyPrefixError'
    }
}
