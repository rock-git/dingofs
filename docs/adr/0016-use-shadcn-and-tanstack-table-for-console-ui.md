# Use shadcn/ui and TanStack Table for the console UI

The management console will use selectively copied shadcn/ui components with Tailwind CSS, and TanStack Table for shared resource-table behavior. This accepts more project-owned frontend source in exchange for smaller embedded output, direct control over the DingoFS visual system, and less dependence on a monolithic UI framework; the console will maintain one shared DataTable abstraction and will not introduce Ant Design, MUI, or another competing component system.
