

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.auth.PropertiesCredentials;
import com.google.gson.Gson;

/**
 * Servlet implementation class getpoints
 */
public class getpoints extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public getpoints() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String keyword = request.getParameter("keyword");
		String startTime = request.getParameter("start");
		String endTime = request.getParameter("end");
		response.setContentType("application/json");
		PrintWriter out = response.getWriter();
		List<SelectResult> list = null;
		Rds rds = Rds.getInstance();
		try {
			while (!rds.isConnected())
				rds.init(readPass());
			list = rds.select(keyword, startTime, endTime);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Gson gson = new Gson();
        out.print(gson.toJson(list));
        out.flush();
	}
	
	private String readPass() {
		InputStream password = Thread.currentThread().getContextClassLoader().getResourceAsStream("pass.ini");
        String pass = null;
        pass = new Scanner(password).next();
        return pass;
	}

}
