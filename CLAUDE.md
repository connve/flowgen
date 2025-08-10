### Documentation & Comments

- Please add relevant comments to code which will serve as documentation.
- In case of functionality changes, please update comments.
- Always use Rust best practices and state of the art.
- Keep comments concise and short.
- Do not use acronyms.
- Use full sentences.
- Document module intros, methods, struct fields, constants etc.
- Do not change any code while documenting.
- Do not comment every function, especially if function meaning is very clear.
- Do comment every function which is part of a public API of a module.
- If possible, please use following traits to structs: #[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)].