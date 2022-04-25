#pragma once
#include <string>

namespace pingcap
{
class Histogram {
    public:
    Histogram() {}
    ~Histogram() {}

    void Clear();
    void Add(double value);
    void Merge(const Histogram& other);
    std::string ToString() const;
    double Median() const;
    double Percentile(double p) const;
    double Average() const;
    double StandardDeviation() const;
    double Minimum() const;
    double Count() const;
    double Maximum() const;
    
    private:
    enum { kNumBuckets = 154 };
    static const double kBucketLimit[kNumBuckets];

    double min_;
    double max_;
    double num_;
    double sum_;
    double sum_squares_;

    double buckets_[kNumBuckets];
};

} //namespace pingcap