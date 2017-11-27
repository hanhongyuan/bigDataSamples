package com.unimas.kstream.output;

import java.util.Objects;

/**
 * element may be null
 *
 * @param <L> left type
 * @param <R> right type
 */
public class ImmutableTuple<L, R> {


    private final L left;
    private final R right;

    public static <L, R> ImmutableTuple<L, R> of(final L left, final R right) {
        return new ImmutableTuple<>(left, right);
    }

    private ImmutableTuple(final L left, final R right) {
        this.left = left;
        this.right = right;
    }


    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ImmutableTuple<?, ?>) {
            final ImmutableTuple<?, ?> other = (ImmutableTuple<?, ?>) obj;
            return Objects.equals(getLeft(), other.getLeft())
                    && Objects.equals(getRight(), other.getRight());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (getLeft() == null ? 0 : getLeft().hashCode()) ^
                (getRight() == null ? 0 : getRight().hashCode());
    }

    @Override
    public String toString() {
        return "(" + getLeft() + "," + getRight() + ")";
    }

}
