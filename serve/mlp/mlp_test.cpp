#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <iostream>
#include "mlp.h"

int main(int argc, char *argv[])
{
    printf("example 1.\n");
    printf("Train a small ANN to the XOR function using backpropagation.\n");

    /* This will make the neural network initialize differently each run. */
    /* If you don't get a good result, try again for a different result. */
    srand(time(0));

    /* Input and expected out data for the XOR function. */
    const std::vector<std::vector<double>> input_raw = {{1.0}, {2.0}, {3.0}, {4.0}};
    const std::vector<std::vector<double>> output_raw = {{2.0}, {4.0}, {5.0}, {6.0}};

    // Normalization parameters
    const double input_scale = 4.0; 
    const double output_scale = 6.0;

    // Normalized data
    std::vector<std::vector<double>> input = {
        {input_raw[0][0] / input_scale},
        {input_raw[1][0] / input_scale},
        {input_raw[2][0] / input_scale},
        {input_raw[3][0] / input_scale}
    };
    
    std::vector<std::vector<double>> output = {
        {output_raw[0][0] / output_scale},
        {output_raw[1][0] / output_scale},
        {output_raw[2][0] / output_scale},
        {output_raw[3][0] / output_scale}
    };

    // print the normalized data
    std::cout << "Normalized Input Data:" << std::endl;
    for (size_t i = 0; i < input.size(); ++i) {
        std::cout << " key=" << input[i][0] << " -> page = " << output[i][0] << "(raw: key=" << input_raw[i][0] << " -> page = " << output_raw[i][0] << ")" << std::endl;
    } // end

    /* New network with 2 inputs,
     * 1 hidden layer of 2 neurons,
     * and 1 output. */
    mlp ann(1, 2, 10, 1);

    /* Train on the four labeled data points many times. */
    for (int i = 0; i < 5000; ++i) {
        for(size_t j = 0; j < input.size(); ++j) {
            ann.train(input[j], output[j], 0.1);
        }
    }

    // Denormalize the results
    for(int i = 0; i < input.size(); ++i) {
        auto result = ann.run(input[i]);
        result[0] *= output_scale;
        auto error = std::abs(result[0] - output_raw[i][0]);
        std::cout << "For input key=" << input_raw[i][0] << ", predicted page=" << result[0] << " (expected page=" << output_raw[i][0] << ", error=" << error << ")" << std::endl; 
    }

    return 0;
}