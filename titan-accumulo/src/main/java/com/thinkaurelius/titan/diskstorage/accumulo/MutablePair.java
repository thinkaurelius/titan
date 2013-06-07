/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.titan.diskstorage.accumulo;

/**
 * A generic class for pairs.
 *
 * @param <T1>
 * @param <T2>
 */
public class MutablePair<T1, T2> {

    protected T1 first = null;
    protected T2 second = null;

    /**
     * Default constructor.
     */
    public MutablePair() {
    }

    /**
     * Constructor
     *
     * @param a operand
     * @param b operand
     */
    public MutablePair(T1 a, T2 b) {
        this.first = a;
        this.second = b;
    }

    /**
     * Constructs a new pair, inferring the type via the passed arguments
     *
     * @param <T1> type for first
     * @param <T2> type for second
     * @param a first element
     * @param b second element
     * @return a new pair containing the passed arguments
     */
    public static <T1, T2> MutablePair<T1, T2> newPair(T1 a, T2 b) {
        return new MutablePair<T1, T2>(a, b);
    }

    /**
     * Replace the first element of the pair.
     *
     * @param a operand
     */
    public void setFirst(T1 a) {
        this.first = a;
    }

    /**
     * Replace the second element of the pair.
     *
     * @param b operand
     */
    public void setSecond(T2 b) {
        this.second = b;
    }

    /**
     * Return the first element stored in the pair.
     *
     * @return T1
     */
    public T1 getFirst() {
        return first;
    }

    /**
     * Return the second element stored in the pair.
     *
     * @return T2
     */
    public T2 getSecond() {
        return second;
    }

    private static boolean equals(Object x, Object y) {
        return (x == null && y == null) || (x != null && x.equals(y));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof MutablePair && equals(first, ((MutablePair) other).first)
                && equals(second, ((MutablePair) other).second);
    }

    @Override
    public int hashCode() {
        if (first == null) {
            return (second == null) ? 0 : second.hashCode() + 1;
        } else if (second == null) {
            return first.hashCode() + 2;
        } else {
            return first.hashCode() * 17 + second.hashCode();
        }
    }

    @Override
    public String toString() {
        return "{" + getFirst() + "," + getSecond() + "}";
    }
}