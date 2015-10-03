package util;

import java.util.Collection;

public interface Consumer<T> {
    void consume(Collection<T> batch);

}