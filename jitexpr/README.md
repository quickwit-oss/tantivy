This is an expression compiler relying on Cranelift.

  UntypedExpr
      ↓ injecting variable types, and type checking
  TypedExpr
      ↓ lowering
  Cranelift IR
      ↓ Cranelift code generation
  Machine code
