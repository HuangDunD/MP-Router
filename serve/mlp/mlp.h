//
// Created by mt on 2025/12/19.
//
#pragma once

#include <cstdio>
#include <vector>
#include <functional>
#include <memory>

#ifndef GENANN_RANDOM
/* We use the following for uniform random numbers between 0 and 1.*/
#define GENANN_RANDOM() (((double)rand())/RAND_MAX)
#endif

class mlp {
public:
    // Activation function type
    using ActivationFunction = std::function<double(double)>;

    // Constructors and Destructor
    mlp(int inputs, int hidden_layers, int hidden, int outputs);
    mlp(const mlp& other);  // Copy constructor
    mlp& operator=(const mlp& other);  // Copy assignment
    ~mlp();

    // Factory method to read from file
    static std::unique_ptr<mlp> readFromFile(FILE* in);

    // Core operations
    std::vector<double> run(const std::vector<double>& inputs);
    void train(const std::vector<double>& inputs,
               const std::vector<double>& desired_outputs,
               double learning_rate);

    // Randomize weights
    void randomize();

    // Save to file
    void write(FILE* out) const;

    // Set activation functions
    void setActivationHidden(ActivationFunction func);
    void setActivationOutput(ActivationFunction func);

    // Getters
    [[nodiscard]] int getInputs() const { return inputs_; }
    [[nodiscard]] int getHiddenLayers() const { return hidden_layers_; }
    [[nodiscard]] int getHidden() const { return hidden_; }
    [[nodiscard]] int getOutputs() const { return outputs_; }
    [[nodiscard]] int getTotalWeights() const { return total_weights_; }
    [[nodiscard]] int getTotalNeurons() const { return total_neurons_; }

    // Static activation functions
    static double actSigmoid(double a);
    static double actSigmoidCached(double a);
    static double actThreshold(double a);
    static double actLinear(double a);
    static void initSigmoidLookup();

private:
    // Network topology
    int inputs_;
    int hidden_layers_;
    int hidden_;
    int outputs_;

    // Network parameters
    int total_weights_;
    int total_neurons_;

    // Activation functions
    ActivationFunction activation_hidden_;
    ActivationFunction activation_output_;

    // Network data (using vectors for RAII)
    std::vector<double> weights_;
    std::vector<double> output_;
    std::vector<double> delta_;

    // Helper methods
    void initialize();
    const double* runInternal(const double* inputs);
    void trainInternal(const double* inputs, const double* desired_outputs, double learning_rate);
};

