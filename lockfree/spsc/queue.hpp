/**************************************************************
 * @file queue.hpp
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

/************************** INCLUDE ***************************/
#ifndef LOCKFREE_QUEUE_HPP
#define LOCKFREE_QUEUE_HPP

#include <atomic>
#include <cstddef>
#include <optional>
#include <type_traits>

namespace lockfree::spsc {

template <typename T, size_t size, template<typename U, size_t s> typename Behavior>
class Queue {
    static_assert(std::is_trivial<T>::value, "The type T must be trivial");
    static_assert(size > 2, "Buffer size must be bigger than 2");
    using ThisClass = Behavior<T, size>;

    class Pusher {
    public:
        Pusher(Pusher &&other) noexcept
            : queue_(std::exchange(other.queue_, nullptr)) {
        }
        ~Pusher() {
            if (queue_) {
                queue_->_w.store(w_next_, std::memory_order_release);
            }
        }
        T* Get() {
            auto [p, next] = queue_->GetImpl();
            if (!p)
                queue_ = nullptr;
            w_next_ = next;
            return p;
        }
    private:
        explicit Pusher(Queue &queue)
            : queue_(&queue){
        }
        Queue *queue_;
        size_t w_next_ = 0;
        friend class Queue;
    };
    /********************** PUBLIC METHODS ************************/
  public:
    Queue();

    /**
     * @brief Adds an element into the queue.
     * Should only be called from the producer thread.
     * @param[in] element
     * @retval Operation success
     */
    bool Push(const T &element);
    Pusher Push();

    /**
     * @brief Removes an element from the queue.
     * Should only be called from the consumer thread.
     * @param[out] element
     * @retval Operation success
     */
    bool Pop(T &element);

    bool IsEmpty() const;
    bool IsFull() const;

    std::optional<T> Pop();

    /********************** PROTECTED MEMBERS ***********************/
protected:
    bool PushImpl(const T &element);
    bool PopImpl(T &element);
    std::optional<T> PopImpl();
    std::pair<T*, size_t> GetImpl();
    consteval static bool IsSizeBase2() {
        return (size & (size - 1)) == 0;
    }

    T _data[size]; /**< Data array */
#if LOCKFREE_CACHE_COHERENT
    alignas(LOCKFREE_CACHELINE_LENGTH) std::atomic_size_t _r; /**< Read index */
    alignas(LOCKFREE_CACHELINE_LENGTH) std::atomic_size_t _w; /**< Write index */
#else
    std::atomic_size_t _r; /**< Read index */
    std::atomic_size_t _w; /**< Write index */
#endif
};

template<typename T, size_t size>
struct NonBlockingQueue : Queue<T, size, NonBlockingQueue> {

};

} // namespace lockfree::spsc


/************************** INCLUDE ***************************/

/* Include the implementation */
#include "queue_impl.hpp"
#include "blocking_queue_impl.hpp"

#endif /* LOCKFREE_QUEUE_HPP */
