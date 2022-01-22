package pagerank;

public class PageRank {

	public static String[] fetchArguments(String[] args) throws Exception {
		if (args == null || args.length < 4) {
			String title = "Page Rank function needs at least 4 arguments:\n";
			String args0 = "args0 : Input Directory of transition.txt\n";
			String args1 = "args1: Input Directory of PageRank.txt\n";
			String args2 = "args2: Input Directory of MatrixCellMultiplication results\n";
			String args3 = "args3: Number of iterations\n";
			String args4 = "args4: value of beta\n";
			String end =   "If only receives 4 args, the last argument will use 0.2f by default."; // using for dead ends
			throw new Exception(title + args0 + args1 + args2 + args3 + args4 + end);
		} else {
			return args;
		}
	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		
		String[] arguments = fetchArguments(args);
		int iter_count = Integer.parseInt(arguments[3]);
		String beta = "0.2";
		if (arguments.length >= 5) {
			beta = arguments[4];
		}

		String directory_transition_matrix = arguments[0];
		String directory_page_rank = arguments[1];
		String mul_results_directory = arguments[2];

		int i = 0;
		while (i < iter_count) {
			String[] mul_arguments = { directory_transition_matrix, directory_page_rank + i, mul_results_directory + i,
					beta };
			String[] sum_arguments = { mul_results_directory + i,
					directory_page_rank + (i + 1), beta };
			OutgoingLinksRankDistribution.main(mul_arguments);
			UpdatePageRank.main(sum_arguments);
			i++;
		}
		long total = System.currentTimeMillis() - start;
		System.out.println("Computaion time taken: " + total / 60000 + " mins");
	}

}
