This is an expression compiler relying on Cranelift.

  UntypedExpr
      ↓ injecting variable types, and type checking
  TypedExpr
      ↓ lowering
  Cranelift IR
      ↓ Cranelift code generation
  Machine code (or assembly)

The project does not rely on cranelifts function call abstraction.
Instead it just manipulates expression, so everything is always inlined.
