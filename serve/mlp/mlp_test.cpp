#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>
#include "mlp.h"

int main(int argc, char *argv[])
{
    printf("example 1.\n");
    printf("Train a small ANN to the XOR function using backpropagation.\n");

    /* This will make the neural network initialize differently each run. */
    /* If you don't get a good result, try again for a different result. */
    srand(time(0));

    /* Input and expected out data for the XOR function. */
    const std::vector<std::vector<double>> input = {{0, 0}, {0, 1}, {1, 0}, {1, 1}};
    const std::vector<std::vector<double>> output = {{0}, {1}, {1}, {0}};

    /* New network with 2 inputs,
     * 1 hidden layer of 2 neurons,
     * and 1 output. */
    mlp ann(2, 1, 2, 1);

    /* Train on the four labeled data points many times. */
    for (int i = 0; i < 500; ++i) {
        ann.train(input[0], output[0], 3);
        ann.train(input[1], output[1], 3);
        ann.train(input[2], output[2], 3);
        ann.train(input[3], output[3], 3);
    }

    /* Run the network and see what it predicts. */
    auto result0 = ann.run(input[0]);
    auto result1 = ann.run(input[1]);
    auto result2 = ann.run(input[2]);
    auto result3 = ann.run(input[3]);

    printf("Output for [%1.f, %1.f] is %1.f.\n", input[0][0], input[0][1], result0[0]);
    printf("Output for [%1.f, %1.f] is %1.f.\n", input[1][0], input[1][1], result1[0]);
    printf("Output for [%1.f, %1.f] is %1.f.\n", input[2][0], input[2][1], result2[0]);
    printf("Output for [%1.f, %1.f] is %1.f.\n", input[3][0], input[3][1], result3[0]);

    return 0;
}