import networkx as nx
import matplotlib.pyplot as plt

def read_edges(file_path):
    """
    读取日志文件，每行格式可能是:
      u,v
      或
      u
    """
    edges = []
    single_nodes = []
    max_lines = 1000
    with open(file_path, 'r') as f:
        for i, line in enumerate(f):
            if i >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            if ',' in line:
                u, v = line.split(',')
                u = u.strip()
                v = v.strip()
                edges.append((u, v))
            else:
                single_nodes.append(line.strip())
    return edges, single_nodes


def visualize_graph(edges, single_nodes):
    G = nx.Graph()

    # 添加边和节点
    G.add_edges_from(edges)
    G.add_nodes_from(single_nodes)  # 确保孤立节点也显示

    print(f"✅ 读取结果: 节点数 {G.number_of_nodes()}, 边数 {G.number_of_edges()}")

    # 布局
    # pos = nx.spring_layout(G, seed=42)  # 随机布局，可换为 nx.kamada_kawai_layout 等

    # pos = nx.circular_layout(G)
    pos = nx.kamada_kawai_layout(G)

    # 绘制节点和边
    plt.figure(figsize=(12, 8))
    nx.draw_networkx_nodes(G, pos, node_size=50, node_color='skyblue')
    nx.draw_networkx_edges(G, pos, width=0.5, alpha=0.6)
    nx.draw_networkx_labels(G, pos, font_size=6)

    plt.title("Graph Visualization", fontsize=14)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig("graph.png", dpi=300)
    print(f"✅ 图已保存到 graph.png")


if __name__ == "__main__":
    # 假设你的日志保存为 graph.txt
    edges, singles = read_edges("/home/hcy/MP-Router/build/serve/test/access_key.log")
    visualize_graph(edges, singles)