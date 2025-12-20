//
// Created by root on 2025/12/19.
//
#include "mlp.h"

#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <utility>

#define LOOKUP_SIZE 4096

namespace {
    const double sigmoid_dom_min = -15.0;
    const double sigmoid_dom_max = 15.0;
    double interval = 0.0;
    double lookup[LOOKUP_SIZE] = {0.0};
    bool lookup_initialized = false;

#ifdef __GNUC__
    #define likely(x)       __builtin_expect(!!(x), 1)
    #define unlikely(x)     __builtin_expect(!!(x), 0)
#else
    #define likely(x)       x
    #define unlikely(x)     x
#endif
}

// Static activation functions
double mlp::actSigmoid(double a) {
    if (a < -45.0) return 0.0;
    if (a > 45.0) return 1.0;
    return 1.0 / (1.0 + std::exp(-a));
}

void mlp::initSigmoidLookup() {
    if (lookup_initialized) return;

    const double f = (sigmoid_dom_max - sigmoid_dom_min) / LOOKUP_SIZE;
    interval = LOOKUP_SIZE / (sigmoid_dom_max - sigmoid_dom_min);

    for (int i = 0; i < LOOKUP_SIZE; ++i) {
        lookup[i] = actSigmoid(sigmoid_dom_min + f * i);
    }

    lookup_initialized = true;
}

double mlp::actSigmoidCached(double a) {
    if (!lookup_initialized) {
        initSigmoidLookup();
    }

    if (a < sigmoid_dom_min) return lookup[0];
    if (a >= sigmoid_dom_max) return lookup[LOOKUP_SIZE - 1];

    auto j = static_cast<size_t>(std::lround((a - sigmoid_dom_min) * interval));

    if (unlikely(j >= LOOKUP_SIZE)) return lookup[LOOKUP_SIZE - 1];

    return lookup[j];
}

double mlp::actLinear(double a) {
    return a;
}

double mlp::actThreshold(double a) {
    return a > 0.0 ? 1.0 : 0.0;
}

// Constructor
mlp::mlp(int inputs, int hidden_layers, int hidden, int outputs)
    : inputs_(inputs)
    , hidden_layers_(hidden_layers)
    , hidden_(hidden)
    , outputs_(outputs)
    , total_weights_(0)
    , total_neurons_(0) {

    if (hidden_layers_ < 0) {
        throw std::invalid_argument("hidden_layers must be non-negative");
    }
    if (inputs_ < 1) {
        throw std::invalid_argument("inputs must be at least 1");
    }
    if (outputs_ < 1) {
        throw std::invalid_argument("outputs must be at least 1");
    }
    if (hidden_layers_ > 0 && hidden_ < 1) {
        throw std::invalid_argument("hidden must be at least 1 when hidden_layers > 0");
    }

    initialize();
}

// Copy constructor
mlp::mlp(const mlp& other) = default;

// Copy assignment
mlp& mlp::operator=(const mlp& other) {
    if (this != &other) {
        inputs_ = other.inputs_;
        hidden_layers_ = other.hidden_layers_;
        hidden_ = other.hidden_;
        outputs_ = other.outputs_;
        total_weights_ = other.total_weights_;
        total_neurons_ = other.total_neurons_;
        activation_hidden_ = other.activation_hidden_;
        activation_output_ = other.activation_output_;
        weights_ = other.weights_;
        output_ = other.output_;
        delta_ = other.delta_;
    }
    return *this;
}

// Destructor
mlp::~mlp() = default;

// Initialize the network
void mlp::initialize() {
    const int hidden_weights = hidden_layers_
        ? (inputs_ + 1) * hidden_ + (hidden_layers_ - 1) * (hidden_ + 1) * hidden_
        : 0;
    const int output_weights = (hidden_layers_ ? (hidden_ + 1) : (inputs_ + 1)) * outputs_;
    total_weights_ = hidden_weights + output_weights;

    total_neurons_ = inputs_ + hidden_ * hidden_layers_ + outputs_;

    // Allocate vectors
    weights_.resize(total_weights_);
    output_.resize(total_neurons_);
    delta_.resize(total_neurons_ - inputs_);

    // Randomize weights
    randomize();

    // Set default activation functions
    activation_hidden_ = [](double a) { return mlp::actSigmoidCached(a); };
    activation_output_ = [](double a) { return mlp::actSigmoidCached(a); };

    // Initialize lookup table
    initSigmoidLookup();
}

// Factory method to read from file
std::unique_ptr<mlp> mlp::readFromFile(FILE* in) {
    int inputs, hidden_layers, hidden, outputs;

    errno = 0;
    int rc = fscanf(in, "%d %d %d %d", &inputs, &hidden_layers, &hidden, &outputs);
    if (rc < 4 || errno != 0) {
        perror("fscanf");
        return nullptr;
    }

    auto ann = std::make_unique<mlp>(inputs, hidden_layers, hidden, outputs);

    for (int i = 0; i < ann->total_weights_; ++i) {
        errno = 0;
        rc = fscanf(in, " %le", &ann->weights_[i]);
        if (rc < 1 || errno != 0) {
            perror("fscanf");
            return nullptr;
        }
    }

    return ann;
}

// Randomize weights
void mlp::randomize() {
    for (int i = 0; i < total_weights_; ++i) {
        double r = GENANN_RANDOM();
        // Sets weights from -0.5 to 0.5
        weights_[i] = r - 0.5;
    }
}

// Set activation functions
void mlp::setActivationHidden(ActivationFunction func) {
    activation_hidden_ = std::move(func);
}

void mlp::setActivationOutput(ActivationFunction func) {
    activation_output_ = std::move(func);
}

// Public run method that returns a vector
std::vector<double> mlp::run(const std::vector<double>& inputs) {
    if (static_cast<int>(inputs.size()) != inputs_) {
        throw std::invalid_argument("Input size mismatch");
    }

    const double* result = runInternal(inputs.data());

    // Return the output layer as a vector
    std::vector<double> output_vec(outputs_);
    for (int i = 0; i < outputs_; ++i) {
        output_vec[i] = result[i];
    }

    return output_vec;
}

// Internal run method (works with raw pointers for efficiency)
const double* mlp::runInternal(const double* inputs) {
    const double* w = weights_.data();
    double* o = output_.data() + inputs_;
    const double* i = output_.data();

    // Copy the inputs to the scratch area, where we also store each neuron's
    // output, for consistency. This way the first layer isn't a special case.
    std::memcpy(output_.data(), inputs, sizeof(double) * inputs_);

    if (!hidden_layers_) {
        double* ret = o;
        for (int j = 0; j < outputs_; ++j) {
            double sum = *w++ * -1.0;
            for (int k = 0; k < inputs_; ++k) {
                sum += *w++ * i[k];
            }
            *o++ = activation_output_(sum);
        }
        return ret;
    }

    // Figure input layer
    for (int j = 0; j < hidden_; ++j) {
        double sum = *w++ * -1.0;
        for (int k = 0; k < inputs_; ++k) {
            sum += *w++ * i[k];
        }
        *o++ = activation_hidden_(sum);
    }

    i += inputs_;

    // Figure hidden layers, if any
    for (int h = 1; h < hidden_layers_; ++h) {
        for (int j = 0; j < hidden_; ++j) {
            double sum = *w++ * -1.0;
            for (int k = 0; k < hidden_; ++k) {
                sum += *w++ * i[k];
            }
            *o++ = activation_hidden_(sum);
        }
        i += hidden_;
    }

    const double* ret = o;

    // Figure output layer
    for (int j = 0; j < outputs_; ++j) {
        double sum = *w++ * -1.0;
        for (int k = 0; k < hidden_; ++k) {
            sum += *w++ * i[k];
        }
        *o++ = activation_output_(sum);
    }

    // Sanity check that we used all weights and wrote all outputs
    assert(w - weights_.data() == total_weights_);
    assert(o - output_.data() == total_neurons_);

    return ret;
}

// Public train method that uses vectors
void mlp::train(const std::vector<double>& inputs,
                   const std::vector<double>& desired_outputs,
                   double learning_rate) {
    if (static_cast<int>(inputs.size()) != inputs_) {
        throw std::invalid_argument("Input size mismatch");
    }
    if (static_cast<int>(desired_outputs.size()) != outputs_) {
        throw std::invalid_argument("Desired output size mismatch");
    }

    trainInternal(inputs.data(), desired_outputs.data(), learning_rate);
}

// Internal train method
void mlp::trainInternal(const double* inputs, const double* desired_outputs, double learning_rate) {
    // To begin with, we must run the network forward
    runInternal(inputs);

    // First set the output layer deltas
    {
        const double* o = output_.data() + inputs_ + hidden_ * hidden_layers_; // First output
        double* d = delta_.data() + hidden_ * hidden_layers_; // First delta
        const double* t = desired_outputs; // First desired output

        // Set output layer deltas
        // Check if output activation is linear
        bool is_linear = false;
        // Simple check: if activation_output_ produces same output as input for a test value
        if (std::abs(activation_output_(1.0) - 1.0) < 1e-9) {
            is_linear = true;
        }

        if (is_linear) {
            for (int j = 0; j < outputs_; ++j) {
                *d++ = *t++ - *o++;
            }
        } else {
            for (int j = 0; j < outputs_; ++j) {
                *d++ = (*t - *o) * *o * (1.0 - *o);
                ++o;
                ++t;
            }
        }
    }

    // Set hidden layer deltas, start on last layer and work backwards
    // Note that loop is skipped in the case of hidden_layers == 0
    for (int h = hidden_layers_ - 1; h >= 0; --h) {
        // Find first output and delta in this layer
        const double* o = output_.data() + inputs_ + (h * hidden_);
        double* d = delta_.data() + (h * hidden_);

        // Find first delta in following layer (which may be hidden or output)
        const double* dd = delta_.data() + ((h + 1) * hidden_);

        // Find first weight in following layer (which may be hidden or output)
        const double* ww = weights_.data() + ((inputs_ + 1) * hidden_) +
                          ((hidden_ + 1) * hidden_ * h);

        for (int j = 0; j < hidden_; ++j) {
            double delta = 0.0;

            for (int k = 0; k < (h == hidden_layers_ - 1 ? outputs_ : hidden_); ++k) {
                const double forward_delta = dd[k];
                const int windex = k * (hidden_ + 1) + (j + 1);
                const double forward_weight = ww[windex];
                delta += forward_delta * forward_weight;
            }

            *d = *o * (1.0 - *o) * delta;
            ++d;
            ++o;
        }
    }

    // Train the outputs
    {
        // Find first output delta
        const double* d = delta_.data() + hidden_ * hidden_layers_; // First output delta

        // Find first weight to first output delta
        double* w = weights_.data() + (hidden_layers_
                                       ? ((inputs_ + 1) * hidden_ + (hidden_ + 1) * hidden_ * (hidden_layers_ - 1))
                                       : 0);

        // Find first output in previous layer
        const double* i = output_.data() + (hidden_layers_
                                           ? (inputs_ + hidden_ * (hidden_layers_ - 1))
                                           : 0);

        // Set output layer weights
        for (int j = 0; j < outputs_; ++j) {
            *w++ += *d * learning_rate * -1.0;
            for (int k = 1; k < (hidden_layers_ ? hidden_ : inputs_) + 1; ++k) {
                *w++ += *d * learning_rate * i[k - 1];
            }
            ++d;
        }

        assert(w - weights_.data() == total_weights_);
    }

    // Train the hidden layers
    for (int h = hidden_layers_ - 1; h >= 0; --h) {
        // Find first delta in this layer
        const double* d = delta_.data() + (h * hidden_);

        // Find first input to this layer
        const double* i = output_.data() + (h ? (inputs_ + hidden_ * (h - 1)) : 0);

        // Find first weight to this layer
        double* w = weights_.data() + (h
                                      ? ((inputs_ + 1) * hidden_ + (hidden_ + 1) * hidden_ * (h - 1))
                                      : 0);

        for (int j = 0; j < hidden_; ++j) {
            *w++ += *d * learning_rate * -1.0;
            for (int k = 1; k < (h == 0 ? inputs_ : hidden_) + 1; ++k) {
                *w++ += *d * learning_rate * i[k - 1];
            }
            ++d;
        }
    }
}

// Save to file
void mlp::write(FILE* out) const {
    fprintf(out, "%d %d %d %d", inputs_, hidden_layers_, hidden_, outputs_);

    for (int i = 0; i < total_weights_; ++i) {
        fprintf(out, " %.20e", weights_[i]);
    }
}


