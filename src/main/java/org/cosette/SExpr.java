package org.cosette;

import kala.collection.Seq;

public sealed interface SExpr {
    record Lst(Seq<SExpr> nodes) implements SExpr {
        @Override public String toString() {
            return nodes.map(Object::toString).joinToString(" ", "(", ")");
        }
    }
    record Sym(String name) implements SExpr {
        @Override public String toString() {
            return name;
        }
    }
    record Str(String value) implements SExpr {
        @Override public String toString() {
            // TODO: Proper escaping
            return "\"" + value + "\"";
        }
    }
    record Bool(boolean value) implements SExpr {
        @Override public String toString() {
            return "(" + (value ? "TRUE" : "FALSE") + ")";
        }
    }
    record Int(long value) implements SExpr {
        @Override public String toString() {
            return Long.toString(value);
        }
    }
    record Real(double value) implements SExpr {
        @Override public String toString() {
            return Double.toString(value);
        }
    }

    static Lst list(SExpr... elems) {
        return new Lst(Seq.of(elems));
    }

    static Sym symbol(String name) {
        return new Sym(name);
    }

    static Str string(String value) {
        return new Str(value);
    }

    static Bool bool(boolean value) {
        return new Bool(value);
    }

    static Int integer(long value) {
        return new Int(value);
    }

    static Real real(double value) {
        return new Real(value);
    }

    static Sym nll() {
        return new Sym("sqlnull");
    }

    static Lst app(String fn, SExpr... args) {
        return app(fn, Seq.of(args));
    }

    static Lst app(String fn, Seq<SExpr> args) {
        return new Lst(args.prepended(symbol(fn)));
    }

    static Sym quoted(String sym) {
        return symbol("'" + sym);
    }
}
