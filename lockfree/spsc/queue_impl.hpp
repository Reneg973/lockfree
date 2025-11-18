/**************************************************************
 * @file queue_impl.hpp
 * @brief A queue implementation written in standard c++11
 * suitable for both low-end microcontrollers all the way
 * to HPC machines. Lock-free for single consumer single
 * producer scenarios.
 **************************************************************/

/**************************************************************
 * Copyright (c) 2023-2025 Djordje Nedic
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to
 * whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall
 * be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
 * KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * This file is part of lockfree
 *
 * Author:          Djordje Nedic <nedic.djordje2@gmail.com>
 * Version:         v2.0.10
 **************************************************************/
#include "queue.hpp"

namespace lockfree::spsc {
/********************** PUBLIC METHODS ************************/

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
Queue<T, size, Behavior>::Queue() : _r(0U), _w(0U) {}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
bool Queue<T, size, Behavior>::Push(const T &element) {
    return static_cast<ThisClass*>(this)->PushImpl(element);
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
auto Queue<T, size, Behavior>::Push() -> Pusher {
    return Pusher(*this);
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
bool Queue<T, size, Behavior>::Pop(T &element) {
    return static_cast<ThisClass*>(this)->PopImpl(element);
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
std::optional<T> Queue<T, size, Behavior>::Pop() {
    return static_cast<ThisClass*>(this)->PopImpl();
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
bool Queue<T, size, Behavior>::IsEmpty() const {
   return _r.load(std::memory_order_relaxed) == _w.load(std::memory_order_relaxed);
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
bool Queue<T, size, Behavior>::IsFull() const {
    auto r = _r.load(std::memory_order_relaxed);
    auto w = _w.load(std::memory_order_relaxed);
    return (w != (size - 1)) * (w + 1) == r;
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
bool Queue<T, size, Behavior>::PushImpl(const T &element) {
    /*
       The full check needs to be performed using the next write index not to
       miss the case when the read index wrapped and write index is at the end
     */
    const size_t w = _w.load(std::memory_order_relaxed);
    size_t w_next = w + 1;
    if (w_next == size) {
        w_next = 0U;
    }

    /* Full check  */
    const size_t r = _r.load(std::memory_order_acquire);
    if (w_next == r) {
        return false;
    }

    /* Place the element */
    _data[w] = element;

    /* Store the next write index */
    _w.store(w_next, std::memory_order_release);
    return true;
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
bool Queue<T, size, Behavior>::PopImpl(T &element) {
    /* Preload indexes with adequate memory ordering */
    size_t r = _r.load(std::memory_order_relaxed);
    const size_t w = _w.load(std::memory_order_acquire);

    /* Empty check */
    if (r == w) {
        return false;
    }

    /* Remove the element */
    element = _data[r];

    /* Increment the read index */
    r++;
    if (r == size) {
        r = 0U;
    }

    /* Store the read index */
    _r.store(r, std::memory_order_release);
    return true;
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
std::optional<T> Queue<T, size, Behavior>::PopImpl() {
    T element;
    bool result = Pop(element);
    if (result)
        return element;

    return std::nullopt;
}

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
std::pair<T*, size_t> Queue<T, size, Behavior>::GetImpl() {
    const size_t w = _w.load(std::memory_order_relaxed);
    size_t w_next = w + 1;
    if (w_next == size) {
        w_next = 0U;
    }

    const size_t r = _r.load(std::memory_order_acquire);
    T *p = (w_next != r) ? &_data[w] : nullptr;
    return {p, w_next};
}

} // namespace lockfree::spsc

