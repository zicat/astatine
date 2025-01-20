package name.zicat.astatine.functions.statistics.percentile;

/** k2. */
public class K2 extends ScaleFunction2 {

  @Override
  public double k(double q, final double normalizer) {
    ScaleFunction2.Function f =
        new ScaleFunction2.Function() {
          @Override
          double apply(double q) {
            return Math.log(q / (1 - q)) * normalizer;
          }
        };
    return ScaleFunction2.limitCall(f, q, 1e-15, 1 - 1e-15);
  }

  @Override
  public double q(double k, double normalizer) {
    double w = Math.exp(k / normalizer);
    return w / (1 + w);
  }

  @Override
  public double max(double q, double compression, double n) {
    return z(compression, n) * q * (1 - q) / compression;
  }

  @Override
  public double max(double q, double normalizer) {
    return q * (1 - q) / normalizer;
  }

  @Override
  public double normalizer(double compression, double n) {
    return compression / z(compression, n);
  }

  private double z(double compression, double n) {
    return 4 * Math.log(n / compression) + 24;
  }
}
