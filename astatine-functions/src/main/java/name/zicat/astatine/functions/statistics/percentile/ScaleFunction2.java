package name.zicat.astatine.functions.statistics.percentile;

/** ScaleFunction2. */
public class ScaleFunction2 {

  /**
   * Converts a quantile to the k-scale. The normalizer value depends on compression and (possibly)
   * number of points in the digest. #normalizer(double, double)
   *
   * @param q The quantile
   * @param normalizer The normalizer value which depends on compression and (possibly) number of
   *     points in the digest.
   * @return The corresponding value of k
   */
  public double k(double q, double normalizer) {
    return 0d;
  }

  /**
   * Computes q as a function of k. This is often faster than finding k as a function of q for some
   * scales.
   *
   * @param k The index value to convert into q scale.
   * @param normalizer The normalizer value which depends on compression and (possibly) number of
   *     points in the digest.
   * @return The value of q that corresponds to k
   */
  public double q(double k, double normalizer) {
    return 0d;
  }

  /**
   * Computes the maximum relative size a cluster can have at quantile q. Note that exactly where
   * within the range spanned by a cluster that q should be isn't clear. That means that this
   * function usually has to be taken at multiple points and the smallest value used.
   *
   * <p>Note that this is the relative size of a cluster. To get the max number of samples in the
   * cluster, multiply this value times the total number of samples in the digest.
   *
   * @param q The quantile
   * @param compression The compression factor, typically delta in the literature
   * @param n The number of samples seen so far in the digest
   * @return The maximum number of samples that can be in the cluster
   */
  public double max(double q, double compression, double n) {
    return 0d;
  }

  /**
   * Computes the maximum relative size a cluster can have at quantile q. Note that exactly where
   * within the range spanned by a cluster that q should be isn't clear. That means that this
   * function usually has to be taken at multiple points and the smallest value used.
   *
   * <p>Note that this is the relative size of a cluster. To get the max number of samples in the
   * cluster, multiply this value times the total number of samples in the digest.
   *
   * @param q The quantile
   * @param normalizer The normalizer value which depends on compression and (possibly) number of
   *     points in the digest.
   * @return The maximum number of samples that can be in the cluster
   */
  public double max(double q, double normalizer) {
    return 0d;
  }

  /**
   * Computes the normalizer given compression and number of points.
   *
   * @param compression The compression parameter for the digest
   * @param n The number of samples seen so far
   * @return The normalizing factor for the scale function
   */
  public double normalizer(double compression, double n) {
    return 0;
  }

  /**
   * Approximates asin to within about 1e-6. This approximation works by breaking the range from 0
   * to 1 into 5 regions for all but the region nearest 1, rational polynomial models get us a very
   * good approximation of asin and by interpolating as we move from region to region, we can
   * guarantee continuity and we happen to get monotonicity as well. for the values near 1, we just
   * use Math.asin as our region "approximation".
   *
   * @param x sin(theta)
   * @return theta
   */
  static double fastAsin(double x) {
    if (x < 0) {
      return -fastAsin(-x);
    } else if (x > 1) {
      return Double.NaN;
    } else {
      // Cutoffs for models. Note that the ranges overlap. In the
      // overlap we do linear interpolation to guarantee the overall
      // result is "nice"
      double c0High = 0.1;
      double c1High = 0.55;
      double c2Low = 0.5;
      double c2High = 0.8;
      double c3Low = 0.75;
      double c3High = 0.9;
      double c4Low = 0.87;
      if (x > c3High) {
        return Math.asin(x);
      } else {
        // the models
        double[] m0 = {
          0.2955302411, 1.2221903614, 0.1488583743, 0.2422015816, -0.3688700895, 0.0733398445
        };
        double[] m1 = {
          -0.0430991920, 0.9594035750, -0.0362312299, 0.1204623351, 0.0457029620, -0.0026025285
        };
        double[] m2 = {
          -0.034873933724,
          1.054796752703,
          -0.194127063385,
          0.283963735636,
          0.023800124916,
          -0.000872727381
        };
        double[] m3 = {
          -0.37588391875,
          2.61991859025,
          -2.48835406886,
          1.48605387425,
          0.00857627492,
          -0.00015802871
        };

        // the parameters for all of the models
        double[] vars = {1, x, x * x, x * x * x, 1 / (1 - x), 1 / (1 - x) / (1 - x)};

        // raw grist for interpolation coefficients
        double x0 = bound((c0High - x) / c0High);
        double x1 = bound((c1High - x) / (c1High - c2Low));
        double x2 = bound((c2High - x) / (c2High - c3Low));
        double x3 = bound((c3High - x) / (c3High - c4Low));

        // interpolation coefficients
        //noinspection UnnecessaryLocalVariable
        double mix0 = x0;
        double mix1 = (1 - x0) * x1;
        double mix2 = (1 - x1) * x2;
        double mix3 = (1 - x2) * x3;
        double mix4 = 1 - x3;

        // now mix all the results together, avoiding extra evaluations
        double r = 0;
        if (mix0 > 0) {
          r += mix0 * eval(m0, vars);
        }
        if (mix1 > 0) {
          r += mix1 * eval(m1, vars);
        }
        if (mix2 > 0) {
          r += mix2 * eval(m2, vars);
        }
        if (mix3 > 0) {
          r += mix3 * eval(m3, vars);
        }
        if (mix4 > 0) {
          // model 4 is just the real deal
          r += mix4 * Math.asin(x);
        }
        return r;
      }
    }
  }

  /** Function. */
  public abstract static class Function {
    abstract double apply(double x);
  }

  static double limitCall(Function f, double x, double low, double high) {
    if (x < low) {
      return f.apply(low);
    } else if (x > high) {
      return f.apply(high);
    } else {
      return f.apply(x);
    }
  }

  private static double eval(double[] model, double[] vars) {
    double r = 0;
    for (int i = 0; i < model.length; i++) {
      r += model[i] * vars[i];
    }
    return r;
  }

  private static double bound(double v) {
    if (v <= 0) {
      return 0;
    } else if (v >= 1) {
      return 1;
    } else {
      return v;
    }
  }
}
