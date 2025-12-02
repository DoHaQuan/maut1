package de.htwberlin.dbtech.aufgaben.ue03;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;


/**
 * Die Klasse realisiert den AusleiheService.
 * 
 * @author Patrick Dohmeier
 */
public class MautServiceImpl implements IMautService {

	private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public float berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
		// TODO Auto-generated method stub

		// Prueft, ob das Fahrzeug registriert ist, sprich ob es "aktiv" ist, ein
		// Fahrzeuggeraet verbaut hat oder im manuellen Verfahren eine offene
		// Buchung des Fahrzeugs vorliegt
		if (!isVehicleRegistered(kennzeichen)) {
			throw new UnkownVehicleException("Das Fahrzeug ist nicht bekannt!-> Mautpreller");
		}
		// 2) Manuelles Verfahren: offene Buchung?
		if (buchungsstatusOffen(kennzeichen, mautAbschnitt)) {
			// 3) Achsenprüfung über MAUTKATEGORIE
			if (!pruefeAchsenManuell(kennzeichen, achszahl)) {
				throw new InvalidVehicleDataException();
			}

			// 4) Update Buchungsstatus und Rückgabe 0f
			setzeBuchungsstatus(kennzeichen, mautAbschnitt); // Test 5
			return 0f;
		} else {
			// 5) Automatikverfahren (nur wenn keine Buchung offen)
			if (isFahrzeugImAutomatikverfahren(kennzeichen)) {
				// Achsenprüfung optional, laut Baum darf direkt berechnet werden
				double laengeKm = ermittleAbschnittslaenge(mautAbschnitt);
				double kostenProKm = ermittleKostenProKm(achszahl);

				float maut = Math.round(laengeKm * kostenProKm * 100) / 100f;


				trageMautErhebungEin(kennzeichen, mautAbschnitt, achszahl, maut); // Test 6
				return maut;
			}
		}
		// 6) Wenn weder Manuell noch Automatik zutrifft
		throw new UnkownVehicleException("Kein Verfahren gefunden.");
	}


	/**
	 * prueft, ob das Fahrzeug bereits registriert und aktiv ist oder eine
	 * manuelle offene Buchung fuer das Fahrzeug vorliegt
	 *
	 * @param kennzeichen , das Kennzeichen des Fahrzeugs
	 * @return true wenn das Fahrzeug registiert ist || false wenn nicht
	 **/
	public boolean isVehicleRegistered(String kennzeichen) {

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try {
			String queryString = "SELECT SUM( ANZAHL ) AS ANZAHL FROM (SELECT COUNT(F.KENNZEICHEN) AS ANZAHL FROM FAHRZEUG F"
					+ " JOIN FAHRZEUGGERAT FZG ON F.FZ_ID = FZG.FZ_ID AND F.ABMELDEDATUM IS NULL AND FZG.STATUS = 'active' "
					+ " AND  F.KENNZEICHEN =  ?  UNION ALL SELECT COUNT(KENNZEICHEN) AS ANZAHL FROM BUCHUNG WHERE"
					+ " KENNZEICHEN = ?  AND B_ID = 1)";
			preparedStatement = getConnection().prepareStatement(queryString);
			preparedStatement.setString(1, kennzeichen);
			preparedStatement.setString(2, kennzeichen);
			resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				return resultSet.getLong("ANZAHL") > 0;
			} else {
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}


	public boolean pruefeAchsenManuell(String kennzeichen, int achsen) throws InvalidVehicleDataException {
		try {
			// Lấy số trục từ BUCHUNG qua MAUTKATEGORIE
			String sql = "SELECT mk.ACHSZAHL " +
					"FROM BUCHUNG b " +
					"JOIN MAUTKATEGORIE mk ON b.KATEGORIE_ID = mk.KATEGORIE_ID " +
					"WHERE b.KENNZEICHEN = ? AND b.B_ID = 1";

			PreparedStatement ps = getConnection().prepareStatement(sql);
			ps.setString(1, kennzeichen);
			ResultSet rs = ps.executeQuery();

			if (rs.next()) {
				String gebuchteAchsenStr = rs.getString("ACHSZAHL");

				// Chuyển sang int để so sánh
				gebuchteAchsenStr = gebuchteAchsenStr.replaceAll("[^0-9]", "").trim();

				// Chuyển sang int để so sánh
				int gebuchteAchsen = Integer.parseInt(gebuchteAchsenStr);

				// Quy tắc so sánh: ≤4 phải bằng, ≥5 thì nhập >=5
				if ((gebuchteAchsen <= 4 && gebuchteAchsen != achsen) ||
						(gebuchteAchsen >= 5 && achsen < 5)) {
					throw new InvalidVehicleDataException();
				}
				return true;
			} else {
				// Không tìm thấy Buchung offen cho xe này
				throw new InvalidVehicleDataException();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// FAHRZEUGVERFAHREN _PRÜFUNG
	public boolean isFahrzeugImAutomatikverfahren(String kennzeichen) {
		try {
			String query = "SELECT COUNT(*) AS ANZAHL FROM FAHRZEUG F "
					+ "JOIN FAHRZEUGGERAT FG ON FG.FZ_ID = F.FZ_ID "
					+ "WHERE F.KENNZEICHEN = ? AND F.ABMELDEDATUM IS NULL AND FG.STATUS = 'active'";
			PreparedStatement ps = getConnection().prepareStatement(query);
			ps.setString(1, kennzeichen);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getInt("ANZAHL") > 0;
			}
			return false;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public boolean isFahrzeugImManuellenVerfahren(String kennzeichen) {
		try {
			String query = "SELECT COUNT(*) AS ANZAHL FROM BUCHUNG WHERE KENNZEICHEN = ? AND B_ID = 1";
			PreparedStatement ps = getConnection().prepareStatement(query);
			ps.setString(1, kennzeichen);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getInt("ANZAHL") > 0;
			}
			return false;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	// Prüfen BuchungsstatusOFFEN
	public boolean buchungsstatusOffen(String kennzeichen, int mautAbschnitt) {
		try {
			String query = "SELECT B_ID FROM BUCHUNG WHERE KENNZEICHEN = ? AND ABSCHNITTS_ID = ?";
			PreparedStatement ps = getConnection().prepareStatement(query);
			ps.setString(1, kennzeichen);
			ps.setInt(2, mautAbschnitt);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				int status = rs.getInt("B_ID");     // NUMBER → getInt
				return status == 1;                 // 1 = offen
			}
			return false;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// Update Buchungstatus
	public void setzeBuchungsstatus(String kennzeichen, int mautAbschnitt) {
		try {
			String query = "UPDATE BUCHUNG SET B_ID = 3 WHERE KENNZEICHEN = ? AND ABSCHNITTS_ID = ?";
			PreparedStatement ps = getConnection().prepareStatement(query);
			ps.setString(1, kennzeichen);
			ps.setInt(2, mautAbschnitt);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public double ermittleAbschnittslaenge(int abschnittId) {
		String sql = "SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
			ps.setInt(1, abschnittId);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getDouble("LAENGE");
				}
				throw new RuntimeException("Abschnitt nicht gefunden: " + abschnittId);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	private String mapAchszahl(int achsen) {
		if (achsen == 1) return "1";
		if (achsen == 2) return "2";
		if (achsen == 3 || achsen == 4) return "= 4";
		return ">= 5";
	}


	public double ermittleKostenProKm(int achsen) {

		String achsString = mapAchszahl(achsen);

		String sql = "SELECT MAUTSATZ_JE_KM FROM MAUTKATEGORIE WHERE ACHSZAHL = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
			ps.setString(1, achsString.trim());

			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getDouble("MAUTSATZ_JE_KM");
				}
				throw new RuntimeException("Mautkategorie nicht gefunden: '" + achsString + "'");
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	public void trageMautErhebungEin(String kennzeichen, int mautAbschnitt, int achszahl, float kosten) {

		System.out.println("DEBUG >>> RUNNING NEW METHOD <<<");

		try (Connection con = getConnection()) {

			// 1. Neue MAUT_ID
			long mautId;
			try (PreparedStatement ps = con.prepareStatement(
					"SELECT COUNT(*) + 1 AS NEWID FROM MAUTERHEBUNG");
				 ResultSet rs = ps.executeQuery()) {

				rs.next();
				mautId = rs.getLong("NEWID");
			}

			// 2. FZG_ID holen (JOIN FAHRZEUG -> FAHRZEUGGERÄT, mit MOD zur Sicherheit)
			int fzgId;
			try (PreparedStatement ps = con.prepareStatement(
					"SELECT MOD(fg.FZG_ID, 10000000000) AS FZG_ID " +
							"FROM FAHRZEUG f " +
							"JOIN FAHRZEUGGERAT fg ON fg.FZ_ID = f.FZ_ID " +
							"WHERE f.KENNZEICHEN = ? AND fg.AUSBAUDATUM IS NULL")) {

				ps.setString(1, kennzeichen);
				try (ResultSet rs = ps.executeQuery()) {
					if (!rs.next()) {
						throw new RuntimeException("Kein aktives Gerät gefunden");
					}
					fzgId = rs.getInt("FZG_ID");
				}
			}





			// 3. KATEGORIE_ID
			String achsString = mapAchszahl(achszahl);
			int katId;

			try (PreparedStatement ps = con.prepareStatement(
					"SELECT KATEGORIE_ID FROM MAUTKATEGORIE WHERE ACHSZAHL = ?")) {

				ps.setString(1, achsString.trim());
				try (ResultSet rs = ps.executeQuery()) {
					if (!rs.next())
						throw new RuntimeException("Mautkategorie nicht gefunden");
					katId = rs.getInt("KATEGORIE_ID");
				}
			}

			// 4. Kosten
			float kostenRounded = Math.round(kosten * 100) / 100f;

			// 5. Insert
			try (PreparedStatement ps = con.prepareStatement(
					"INSERT INTO MAUTERHEBUNG " +
							"(MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) " +
							"VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)")) {

				ps.setLong(1, mautId);
				ps.setInt(2, mautAbschnitt);
				ps.setInt(3, fzgId);   // >>> THIS IS CORRECT NOW
				ps.setInt(4, katId);
				ps.setFloat(5, kostenRounded);

				ps.executeUpdate();
			}

		} catch (Exception e) {
			System.out.println("DEBUG ERROR: " + e);
			throw new RuntimeException("Fehler beim Eintragen der Mauterhebung", e);
		}
	}

}















