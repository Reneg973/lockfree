#include "queue.hpp"

namespace lockfree::spsc {
/********************** PUBLIC METHODS ************************/

template <typename T, size_t size>
struct BlockingQueue : Queue<T, size, BlockingQueue> {
    using BaseClass = Queue<T, size, BlockingQueue>;
    bool PushImpl(const T &element);
    bool PopImpl(T &element);
    std::pair<T *, size_t> GetImpl();
};

template <typename T, size_t size>
bool BlockingQueue<T, size>::PushImpl(const T &element) {
    const size_t w = BaseClass::_w.load(std::memory_order_relaxed);
    const size_t w_next = BaseClass::IsSizeBase2()
            ? (w+1) & (size - 1)
            : (w != size - 1) ? (w + 1) : 0;

    if (w_next == BaseClass::_r.load(std::memory_order_acquire))
        BaseClass::_r.wait(w_next);

    BaseClass::_data[w] = element;

    BaseClass::_w.store(w_next, std::memory_order_release);
    BaseClass::_w.notify_one();
    return true;
}

template <typename T, size_t size>
bool BlockingQueue<T, size>::PopImpl(T &element) {
    size_t r = BaseClass::_r.load(std::memory_order_relaxed);

    if (r == BaseClass::_w.load(std::memory_order_acquire)) {
        BaseClass::_w.wait(r);
    }

    element = BaseClass::_data[r];

    // Increment and wrap the read index
    r = BaseClass::IsSizeBase2()
            ? (r + 1) & (size - 1)
            : (r != size - 1) ? (r + 1) : 0;

    // Store the read index
    BaseClass::_r.store(r, std::memory_order_release);
    BaseClass::_r.notify_one();
    return true;
}

template <typename T, size_t size>
std::pair<T *, size_t> BlockingQueue<T, size>::GetImpl() {
    const size_t w = BaseClass::_w.load(std::memory_order_relaxed);
    const size_t w_next = BaseClass::IsSizeBase2() ? (w + 1) & (size - 1)
                          : (w != size - 1)        ? (w + 1)
                                                   : 0;

    if (w_next == BaseClass::_r.load(std::memory_order_acquire))
        BaseClass::_r.wait(w_next);

    return {&BaseClass::_data[w], w_next};
}

} // namespace lockfree::spsc