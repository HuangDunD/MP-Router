/**
 * @file
 * @brief This is an implementation of an min indexed priority queue,
 * which, unlike a regular priority queue, has key value pairs, which 
 * can be looked up in O(1), just like a hashmap. lower priority numbers
 * refer to higher priority.
 * @details
 * As mentioned in the brief, an indexed priority queue operates on 
 * the basis of a hashmap, providing its benefit of having O(1) searching
 * and log(N) modification/removal of ANY key in the queue (not only the front)
 * but also the benefits of a priority queue for polling min values in 
 * logarithmic time or having its heap properties. The keys in this 
 * implementation are the unique identifiers or stored values of the 
 * queue, whilst the values refer to the comparable that will be used
 * to prioritise different items. 
 * refer to william fiset's video for a more detailed explanation:
 * https://youtu.be/jND_WJ8r7FE
 * @author [ron0studios](https://github.com/ron0studios)
 */


#include <vector> /// std::vector
#include <utility> /// std::pair
#include <unordered_map> /// std::std::unordered_map
#include <functional> /// std::less, std::greater
#include <cmath> /// std::floor

#include <iostream> /// debug
#include <string> /// tests
#include <cassert>   /// for assert


/**
 * @brief This is the class used to instantiate the indexed priority queue
 * @tparam T1 this is the identifier value for the queue, and is what will be
 * stored in the data-structure. Please make sure this is a HASHABLE type!
 * @tparam T2 this is the priority value for the queue, used to prioritise
 * certain items over other. Please make sure this is both HASHABLE and 
 * COMPARABLE!
 */
template <class T1 = int, class T2 = int, class Comp = std::less<T2>>
class IPQ
{
	private:
		std::unordered_map<T1,T2> val; ///< key : val
		std::unordered_map<T1,int> pm; ///< key : heap index
		std::unordered_map<int,T1> im; ///< heap index : key
		Comp comp; ///< comparator defining heap priority (default: min-heap)
        int size_ = 0; ///< the size_ of the heap

	public:
						
		/**
		 * @brief Constructor for IPQ
		 * @param keyval this is a vector of key value pairs used initially (optional)
		 */
		IPQ(std::vector<std::pair<T1,T2>> keyval = std::vector<std::pair<T1,T2>>())
		{
			for(auto const &i : keyval){
				insert(i.first, i.second);
			}
		}

        /**
         * @brief get the current size_ of the priority queue
         * @returns the size_ as an integer
         */
        int size() {
            return size_;
        }

        /**
         * @brief check if the priority queue is empty
         * @returns true if empty, false otherwise
         */
        bool empty() {
            return size_ == 0;
        }

		/**
		 * @brief check if the queue contains a certain key
		 * @param key the key to check for
	     */
		bool contains(T1 key)
		{
			if(pm.count(key)) // c++11
				return true;
			return false;
		}

		/**
		 * @brief check the position of a certain key
		 * @param key the key to check for
	     */
		int get_item_heap_idx(T1 key)
		{
			if(pm.count(key)) // c++11
				return pm[key];
			return -1;
		}

		/**
		 * @brief inserts a key-value pair into the heap
		 * @param key the key to insert
		 * @param value the key's associated priority
	     */
		void insert(T1 key, T2 value)
		{
			// add node to bottom right of heap
			val[key] = value;
			pm[key] = size_;
			im[size_] = key;

			//swim
			swim(size_);
			size_++;
		}

		/**
		 * @brief peeks the front of the priority queue
		 * @returns a key value pair of T1 and T2
	     */
		std::pair<T1,T2> peek()
		{
			std::pair<T1,T2> x = std::make_pair(im[0], val[im[0]]);
			return x;
		}

		/**
		 * @brief removes the front element of the priority queue
		 * @returns a key value pair of the front element before popping
	     */
		std::pair<T1,T2> pop()
		{
			std::pair<T1,T2> x = std::make_pair(im[0], val[im[0]]);
			remove(im[0]); // O(logn)
			return x;
		}

		/**
		 * @brief removes a given key of the priority queue
		 * @param key the key to remove
	     */
		void remove(T1 key)
		{
			if(!contains(key)) return;

			int pos = pm[key];
			swap(pos,size_-1);
			size_--;

			val.erase(key);
			pm.erase(key);
			im.erase(size_);

			sink(pos);
			swim(pos);
		}

		/**
		 * @brief updates the priority value of an existing key in the queue
		 * @param key the key whose priority will be updated
		 * @param value the new priority value associated with the key
	     */
		void update(T1 key, T2 value)
		{
			if(!contains(key)) return; // no-op if key not present

			int pos = pm[key];
			T2 old = val[key];
			val[key] = value;

			// Restore heap property based on comparator priority
			if (comp(value, old)) {
				// new value has higher priority than old
				swim(pos);
			} else if (comp(old, value)) {
				// new value has lower priority than old
				sink(pos);
			}
			// if equal, heap order unchanged
		}

	private:
		
		/**
		 * @brief swaps two key value pairs in the heap
		 * @param i the heap index of the first pair
		 * @param j the heap index ot the second pair
	     */
		void swap(int i, int j)
		{
			pm[im[j]] = i;
			pm[im[i]] = j;
			T1 tmp = im[i];
			im[i] = im[j];
			im[j] = tmp;
		}
		
		/**
		 * @brief An algorithm to set a misplaced item in a heap
		 * to its correct place by swapping with children.
		 * @param node the heap index to sink
	     */
		void sink(int node) 
		{
			while(true)
			{
				// children of a heap are represented as such
				int left = (2*node)+1; 
				int right = (2*node)+2;
				int best;

				if(left>=size_ && right>=size_)
					break;

				if(right >= size_)
					best = left;
				else if(left >= size_)
					best = right;
				else
					best = comp(val[im[left]], val[im[right]]) ? left : right;

				// If node already has higher priority than best child, stop
				if(!comp(val[im[best]], val[im[node]]))
					break;

				swap(best,node);
				node = best;

			}
		}

		/**
		 * @brief An algorithm to set a misplaced item in a heap
		 * to its correct place by swapping with parents.
		 * @param node the heap index to swim
	     */
		void swim(int node) 
		{
			int i = std::floor((node-1)/2);
			while(i >= 0 && comp(val[im[node]], val[im[i]]))
			{
				swap(i,node);
				node = i;
				i = std::floor((node-1)/2);
			}
		}
};

/**
 * @brief Dense Indexed Priority Queue implementation optimized for contiguous integer keys 0..N-1.
 * Uses std::vector instead of std::unordered_map for internal storage.
 */
template <class T2 = int, class Comp = std::less<T2>>
class DenseIPQ
{
	private:
		// Vectors indexed by key (0..capacity-1) for val and pm
		std::vector<T2> val;  ///< key -> value (priority)
		std::vector<int> pm;  ///< key -> heap_index, -1 if not in heap

		// Vector indexed by heap_index (0..size_-1)
		std::vector<int> im;  ///< heap_index -> key
		
		Comp comp;
        int size_ = 0;

	public:
		DenseIPQ(int capacity = 0)
		{
            if (capacity > 0) reserve(capacity);
		}

        void reserve(int capacity) {
            val.resize(capacity);
            pm.resize(capacity, -1);
            im.resize(capacity);
        }

        int size() const { return size_; }
        bool empty() const { return size_ == 0; }

		bool contains(int key) const
		{
            if (key < 0 || key >= pm.size()) return false;
			return pm[key] != -1;
		}

		int get_item_heap_idx(int key) const
		{
            if (contains(key)) return pm[key];
			return -1;
		}

		void insert(int key, T2 value)
		{
            if (key >= val.size()) {
                // Auto-grow if needed, though pre-reserve is better
                int new_cap = std::max(key + 1, (int)val.size() * 2);
                val.resize(new_cap);
                pm.resize(new_cap, -1);
                im.resize(new_cap);
            }
            
			val[key] = value;
			pm[key] = size_;
			im[size_] = key;
			swim(size_);
			size_++;
		}

		std::pair<int,T2> peek()
		{
            // assert(size_ > 0);
			return {im[0], val[im[0]]};
		}

		std::pair<int,T2> pop()
		{
            // assert(size_ > 0);
            int ret_key = im[0];
            T2 ret_val = val[ret_key];
			remove(ret_key); 
			return {ret_key, ret_val};
		}

		void remove(int key)
		{
			if(!contains(key)) return;

			int pos = pm[key];
			swap(pos, size_-1);
			size_--;

			pm[key] = -1;
            // No need to erase from val/im, just overwrite later
			
			sink(pos);
			swim(pos);
		}

		void update(int key, T2 value)
		{
			if(!contains(key)) return;

			int pos = pm[key];
			T2 old = val[key];
			val[key] = value;

			if (comp(value, old)) {
				swim(pos);
			} else if (comp(old, value)) {
				sink(pos);
			}
		}

	private:
		void swap(int i, int j)
		{
			pm[im[j]] = i;
			pm[im[i]] = j;
			int tmp = im[i];
			im[i] = im[j];
			im[j] = tmp;
		}
		
		void sink(int node) 
		{
			while(true)
			{
				int left = (2*node)+1; 
				int right = (2*node)+2;
				int best;

				if(left >= size_) break;

				if(right >= size_)
					best = left;
				else
					best = comp(val[im[left]], val[im[right]]) ? left : right;

				if(!comp(val[im[best]], val[im[node]]))
					break;

				swap(best,node);
				node = best;
			}
		}

		void swim(int node) 
		{
			int i = (node-1)/2;
			while(node > 0 && comp(val[im[node]], val[im[i]]))
			{
				swap(i,node);
				node = i;
				i = (node-1)/2;
			}
		}
};
