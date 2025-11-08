package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

/**
 * Die Klasse realisiert die Mautverwaltung.
 * 
 * @author Patrick Dohmeier
 */
public class MautVerwaltungImpl implements IMautVerwaltung {

	private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
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
	public String getStatusForOnBoardUnit(long fzg_id) {
		// TODO Auto-generated method stub

		PreparedStatement preparedstatement = null;
		ResultSet resultSet = null;
		String query = "SELECT STATUS FROM Fahrzeuggerat WHERE FZG_ID = ?";
		String res = "";
		try {

			preparedstatement = getConnection().prepareStatement(query);
			preparedstatement.setLong(1, fzg_id);
			resultSet = preparedstatement.executeQuery();
			if (resultSet.next()) {
				res = resultSet.getString("STATUS");
			}
		} catch (SQLException exp) {
			throw new RuntimeException(exp);
		} catch (NullPointerException exp) {
			throw new RuntimeException(exp);
		}
		return res;
	}

	@Override
	public int getUsernumber(int maut_id) {
		// TODO Auto-generated method stub
		int nutzer_ID = -1;

		ResultSet resultSet = null;
		String sql = """
             SELECT n.nutzer_id
			 FROM mauterhebung m
			 JOIN fahrzeuggerat fg ON m.fzg_id = fg.fzg_id
			 JOIN fahrzeug f ON fg.fz_id = f.fz_id
			 JOIN nutzer n ON f.nutzer_id = n.nutzer_id
             WHERE m.maut_id = ?
		""";
		 try {


PreparedStatement ps = getConnection().prepareStatement(sql);
ps.setInt(1,maut_id);

ResultSet rs =ps.executeQuery();
if(rs.next()){
	nutzer_ID = rs.getInt("nutzer_id");
}
		 }catch(SQLException e){
			 throw new RuntimeException(e.getMessage());
		 }

		return nutzer_ID;
	}

	@Override
	public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
			int gewicht, String zulassungsland) {
		// TODO Auto-generated method stub
		PreparedStatement psFahrzeug = null;
		String insertSQL = """ 
				Insert into FAHRZEUG (fz_id,sskl_id,nutzer_id, kennzeichen,fin,achsen,gewicht,zulassungsland)
				Values (?,?,?,?,?,?,?,?)
				""";
		try{
			psFahrzeug= getConnection().prepareStatement(insertSQL);
			psFahrzeug.setLong(1,fz_id);
			psFahrzeug.setInt(2, sskl_id);
			psFahrzeug.setInt(3, nutzer_id);
			psFahrzeug.setString(4, kennzeichen);
			psFahrzeug.setString(5, fin);
			psFahrzeug.setInt(6, achsen);
			psFahrzeug.setInt(7, gewicht);
			psFahrzeug.setString(8, zulassungsland);
			psFahrzeug.executeUpdate();
			psFahrzeug.close();
		} catch (SQLException e) {
			throw new RuntimeException("Fehler beim Registrieren des Fahrzeugs: " + e.getMessage());
		} finally {
			try {
				if (psFahrzeug != null) psFahrzeug.close();
			} catch (SQLException e) {
				// Log lỗi đóng kết nối
			}
		}
	}




	@Override
	public void updateStatusForOnBoardUnit(long fzg_id, String status) {
		// TODO Auto-generated method stub
		PreparedStatement ps1 = null ;
		String sql1 = "UPDATE  FAHRZEUGGERAT SET STATUS = ? WHERE FZG_ID = ?";
		try {
			ps1 = getConnection().prepareStatement(sql1);
			ps1.setString(1, status);
			ps1.setLong(2, fzg_id);

			ps1.executeUpdate();
			ps1.close();
		}catch (SQLException e){
			throw new RuntimeException(e.getMessage());
		}


		}





	@Override
	public void deleteVehicle(long fz_id) {
		// TODO Auto-generated method stub
		PreparedStatement ps2 = null;
		String sql2 = "DELETE FROM FAHRZEUG WHERE FZ_ID = ?";
		try{
			ps2 = getConnection().prepareStatement(sql2);
			ps2.setLong(1,fz_id);
			ps2.executeUpdate();
			ps2.close();
		}catch (SQLException e){
			throw new RuntimeException(e.getMessage());


		}

	}

	@Override
	public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
		// TODO Auto-generated method stub
		PreparedStatement ps4 = null;
		List<Mautabschnitt> abschnittList = new ArrayList<>();
		String sql4 = """
        SELECT abschnittstyp
        FROM mautabschnitt
        WHERE abschnittstyp = ?
    """;
		try{
			ps4 = getConnection().prepareStatement(sql4);
			ps4.setString(1, abschnittstyp);
			ResultSet rs = ps4.executeQuery();
			while (rs.next()) {
				Mautabschnitt abschnitt = new Mautabschnitt();
				abschnitt.setAbschnittstyp(rs.getString("abschnittstyp"));
				abschnittList.add(abschnitt);
			}

		} catch (SQLException e) {
			throw new RuntimeException("Fehler beim Abrufen der Mautabschnitte: " + e.getMessage());
		}

		return abschnittList;
	}

		}



