import matplotlib.pyplot as plt

experiments = ['1 DataNode\n(Base)', '1 DataNode\n(Opt)', '3 DataNode\n(Base)', '3 DataNode\n(Opt)']

# Время выполнения (в секундах)
time_seconds = [5.50, 6.15, 5.53, 6.80]

# Использование оперативной памяти (в мб)
memory_mb = [39.09, 44.85, 47.86, 47.78]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# 1. График времени
bars1 = ax1.bar(experiments, time_seconds, color=['#ff9999', '#66b3ff', '#ff9999', '#66b3ff'])
ax1.set_title('Сравнение времени выполнения (сек)', fontsize=14, pad=15)
ax1.set_ylabel('Секунды')
ax1.grid(axis='y', linestyle='--', alpha=0.7)
for bar in bars1:
    yval = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2, yval + 0.1, f"{yval:.2f}", ha='center', va='bottom', fontweight='bold')

# 2. График памяти
bars2 = ax2.bar(experiments, memory_mb, color=['#99ff99', '#ffcc99', '#99ff99', '#ffcc99'])
ax2.set_title('Сравнение использования RAM драйвером (MB)', fontsize=14, pad=15)
ax2.set_ylabel('Мегабайты')
ax2.grid(axis='y', linestyle='--', alpha=0.7)
for bar in bars2:
    yval = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2, yval + 1, f"{yval:.2f}", ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.show()