# Useful development tools

This readme contains some hints on how to set up some optional development tools.

## ccls

[ccls](https://github.com/MaskRay/ccls) is a c/c++ language server. It requires some setup
to work well. There are different ways to do it but here's what works for me:
1. Make a common parent directory for all your common neon projects. (for example, `~/src/neondatabase/`)
2. Go to `vendor/postgres-v15`
3. Run `make clean && ./configure`
4. Install [bear](https://github.com/rizsotto/Bear), and run `bear -- make -j4`
5. Copy the generated `compile_commands.json` to `~/src/neondatabase` (or equivalent)
6. Run `touch ~/src/neondatabase/.ccls-root` this will make the `compile_commands.json` file discoverable in all subdirectories

With this setup you will get decent lsp mileage inside the postgres repo, and also any postgres extensions that you put in `~/src/neondatabase/`, like `pg_embedding`, or inside `~/src/neondatabase/neon/pgxn` as well.

Some additional tips for various IDEs:

### Emacs

To improve performance: `(setq lsp-lens-enable nil)`
