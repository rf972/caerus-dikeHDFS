#include <iostream>
#include <omp.h>

int main(int argc, char** argv){
    omp_set_dynamic(1);
    omp_set_num_threads(3);
    #pragma omp parallel
    {
        std::cout << "Create readers with " << omp_get_num_threads() << " threads dynamic " << omp_get_dynamic() << std::endl;
        #pragma omp for
        for(int i = 0; i < 10; i++) {
            std::cout << "Create reader for Column " << i << " tid " << omp_get_thread_num() << std::endl;
        }
    }

    return 0;
}


#if 0
export OMP_NUM_THREADS=4
export OMP_DYNAMIC=true
export OMP_NESTED=true
#endif