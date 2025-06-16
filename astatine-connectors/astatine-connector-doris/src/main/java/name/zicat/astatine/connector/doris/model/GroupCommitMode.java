package name.zicat.astatine.connector.doris.model;

import static name.zicat.astatine.connector.doris.util.DorisUtils.HEAD_KEY_LABEL;
import static name.zicat.astatine.connector.doris.util.DorisUtils.HEAD_KEY_GROUP_COMMIT;

/** GroupCommitMode. */
public enum GroupCommitMode {
  ASYNC_MODE {
    @Override
    public void head(String label, HeaderHandler headerHandler) {
      headerHandler.dealHeader(HEAD_KEY_GROUP_COMMIT, toString());
    }

    @Override
    public String toString() {
      return "async_mode";
    }
  },
  SYNC_MODE {
    @Override
    public void head(String label, HeaderHandler headerHandler) {
      headerHandler.dealHeader(HEAD_KEY_GROUP_COMMIT, toString());
    }

    @Override
    public String toString() {
      return "sync_mode";
    }
  },
  OFF_MODE {
    @Override
    public void head(String label, HeaderHandler headerHandler) {
      headerHandler.dealHeader(HEAD_KEY_LABEL, label);
    }

    @Override
    public String toString() {
      return "off_mode";
    }
  };

  /**
   * set head with diff group commit mode.
   *
   * @param label label
   */
  public abstract void head(String label, HeaderHandler headerHandler);

  /** HeaderHandler. */
  public interface HeaderHandler {

    void dealHeader(String key, String value);
  }
}
